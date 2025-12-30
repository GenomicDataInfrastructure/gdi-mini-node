import logging
import time
from collections import OrderedDict
from typing import Optional, List

import httpx
import jwt
from jwt import InvalidTokenError, ExpiredSignatureError
from jwt import PyJWK

_log = logging.getLogger(__name__)


class LRUCache:
    """Simple LRU cache storing JWT (str) → bool results.
    The cache applies time-checking to avoid returning expired cache items."""

    def __init__(self, capacity: int, valid_secs: int):
        self.capacity = capacity
        self.data = OrderedDict()
        self.valid_secs = valid_secs

    def get(self, key: str) -> bool | None:
        if key in self.data:
            self.data.move_to_end(key)
            valid, ts = self.data[key]
            if time.time() - ts <= self.valid_secs:
                return valid
            # Remove expired key:
            self.data.pop(key)
        return None

    def put(self, key, value):
        self.data[key] = (value, time.time())
        self.data.move_to_end(key)
        if len(self.data) > self.capacity:
            self.data.popitem(last=False)


class OidcVerifier:
    def __init__(
            self,
            issuer: str,
            client_id: str,
            client_secret: str,
            required_visas: list[dict[str, str]] | None,
    ):
        self._issuer = issuer.rstrip("/")
        self._client_id = client_id
        self._client_secret = client_secret
        self._required_visas = required_visas

        # Information from the well-known configuration (JSON):
        self._userinfo_endpoint: Optional[str] = None
        self._jwk: Optional[PyJWK] = None

        # Request sending and retry behavior (unit: seconds)
        self._request_timeout = 5
        self._backoff_delay = 10

        # For caching JWT (str) validation results
        self._lru = LRUCache(capacity=1000, valid_secs=60)

    # ------------------------------------------------------
    # Initialization of the OIDC client
    # ------------------------------------------------------
    def init(self) -> None:
        well_known_url = self._issuer.rstrip(
            '/') + "/.well-known/openid-configuration"

        oidc_conf = self._retry_json_fetch(well_known_url, tries=5)
        if oidc_conf is None:
            raise RuntimeError(
                f"Unable to fetch OIDC configuration from {well_known_url}")

        self._userinfo_endpoint = oidc_conf.get("userinfo_endpoint")
        jwks_uri = oidc_conf.get("jwks_uri")
        if not jwks_uri:
            raise RuntimeError("OIDC configuration did not expose 'jwks_uri'")

        jwks = self._retry_json_fetch(jwks_uri, tries=5)
        if jwks is None:
            raise RuntimeError(f"Unable to fetch JWKS from [{jwks_uri}]")

        keys = jwks.get("keys")
        if not keys:
            raise RuntimeError(f"JWKS from [{jwks_uri}] has no keys.")

        self._jwk = self._get_jwk(jwks_uri)

    # ------------------------------------------------------
    # Verify a JWT from the Authorization header
    # ------------------------------------------------------
    def verify(self, token: str) -> bool:
        # We expect JWT values to be longer than 100 characters:
        if len(token) <= 100:
            _log.debug("Received an invalid Bearer token: [%s]", token)
            return False

        cached_validation_result = self._lru.get(token)
        if cached_validation_result is not None:
            _log.debug(
                "Using a cached JWT validation result: [%s]",
               cached_validation_result,
            )
            return cached_validation_result

        if self._jwk is None:
            raise RuntimeError("Verifier not initialized: call init() first.")

        try:
            claims = jwt.decode(
                token,
                key=self._jwk.key,
                algorithms=[self._jwk.algorithm_name],
                options={
                    "verify_exp": True, "verify_iat": True, "verify_aud": False,
                },
            )
        except InvalidTokenError as e:
            _log.debug("JWT decoding failed: [%s]", repr(e))
            self._lru.put(token, False)
            return False

        sub = claims.get("sub")
        if not sub:
            _log.debug("This JWT does not include 'sub': %s", token)
            self._lru.put(token, False)
            return False

        passport = claims.get("ga4gh_passport_v1")
        valid = self._check_passport(sub, passport)

        _log.info(
            "Validation outcome for the JWT token [sub=%s]: %s.", sub, valid,
        )

        self._lru.put(token, valid)
        return valid

    # ------------------------------------------------------
    # GA4GH Passport checking
    # ------------------------------------------------------
    def _check_passport(self, sub: str,
                        passport_claim: list[str] | None) -> bool:
        if self._required_visas is None or len(self._required_visas) == 0:
            # Skip passport checking if there are no required visas configured.
            _log.debug("Skipping passport validation (visas not required).")
            return True

        if not isinstance(passport_claim, list) or len(passport_claim) == 0:
            _log.warning("ga4gh_passport_v1 claim is empty for subject [%s]",
                         sub)
            return False

        return self._check_visas(sub, passport_claim)

    # ------------------------------------------------------
    # GA4GH Visa checking
    # ------------------------------------------------------
    def _check_visas(self, subject: str, visa_jwts: List[str]) -> bool:
        expected_visas = list(self._required_visas)
        for visa_jwt in visa_jwts:
            self._check_visa(subject, visa_jwt, expected_visas)
            if len(expected_visas) == 0:
                return True
        _log.info("User [%s] does not have the required GA4GH Visas.", subject)
        return False

    def _check_visa(self, subject: str, visa_jwt: str,
                    expected_visas: list[dict[str, str]]) -> None:
        try:
            visa_header = jwt.get_unverified_header(visa_jwt)
            visa_claims = jwt.decode(
                visa_jwt,
                options={
                    "verify_signature": False,
                    "verify_exp": True,
                    "verify_iat": True,
                },
            )
        except (ExpiredSignatureError, InvalidTokenError):
            return

        # Also verify the subject of the Visa:
        visa_subject = visa_claims.get("sub")
        if visa_subject != subject:
            _log.warning(
                "GA4GH Visa subject [%s] is not the same as in the JWT [%s]: %s",
                visa_subject, subject, visa_jwt
            )
            return

        visa_obj = visa_claims.get("ga4gh_visa_v1")
        matched_visas = self._match_visa_claims(visa_obj, expected_visas)
        if len(matched_visas) == 0:
            return

        if self._verify_visa_signature(subject, visa_jwt, visa_header,
                                       visa_obj):
            for visa_check in matched_visas:
                expected_visas.remove(visa_check)

    @staticmethod
    def _match_visa_claims(
            visa_obj: dict, expected_visas: list[dict[str, str]],
    ) -> list[dict[str, str]]:
        if not isinstance(visa_obj, dict):
            _log.warning("Visa is not a dict: %s", type(visa_obj))
            return []

        matched_visas = []
        for visa_check in expected_visas:
            full_match = True
            for claim, value in visa_check.items():
                if visa_obj.get(claim) != value:
                    full_match = False
            if full_match:
                matched_visas.append(visa_check)
        return matched_visas

    def _verify_visa_signature(
            self, subject: str, visa_jwt: str, visa_header: dict, visa_obj: dict
    ) -> bool:
        source = visa_obj.get('source')
        asserted = visa_obj.get('asserted')
        by = visa_obj.get('by')
        msg_has_visa = (
            f"Subject '{subject}' has the required visa from [{source}] issued "
            f"at {asserted} by '{by}'")

        jku = visa_header.get("jku")
        if not jku:
            _log.warning(f"{msg_has_visa} but no 'jku' in header for verification.")
            return False

        try:
            visa_jwk = self._get_jwk(jku)
            jwt.decode(
                visa_jwt,
                key=visa_jwk.key,
                algorithms=[visa_jwk.algorithm_name],
                options={
                    "verify_signature": True,
                    "verify_iat": False,
                    "verify_exp": False,
                },
            )

            _log.info(f"{msg_has_visa}.")
            return True

        except Exception as e:
            _log.warning(f"{msg_has_visa} but JWT signature could not be verified: {e}")
            return False

    # ------------------------------------------------------
    # Helpers for fetching JWT resources
    # ------------------------------------------------------
    def _get_jwk(self, jwks_uri) -> PyJWK:
        jwks = self._retry_json_fetch(jwks_uri, tries=5)
        if jwks is None:
            raise RuntimeError(f"Unable to fetch JWKS from [{jwks_uri}]")

        keys = jwks.get("keys")
        if not keys:
            raise RuntimeError(f"JWKS from [{jwks_uri}] has no keys.")

        return PyJWK.from_dict(keys[0])

    def _retry_json_fetch(self, url: str, tries: int = 5):
        _log.info("Fetching JSON from [%s]", url)
        for attempt in range(1, tries + 1):
            try:
                with httpx.Client(timeout=self._request_timeout) as client:
                    resp = client.get(url)
                if resp.status_code == 200:
                    return resp.json()
                else:
                    _log.warning(
                        "Attempt %d failed: HTTP %d", attempt, resp.status_code
                    )
            except Exception as e:
                _log.warning("Attempt %d failed: %s", attempt, e)

            time.sleep(self._backoff_delay)

        return None
