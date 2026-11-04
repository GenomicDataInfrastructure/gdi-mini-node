import logging
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Callable, List, Optional

import httpx
import jwt
from jwt import InvalidTokenError, ExpiredSignatureError, PyJWTError
from jwt import PyJWK

from mini_node.beacon.config import BeaconVisaIssuer

_log = logging.getLogger(__name__)

@dataclass
class _JwksCache:
    uri: str
    keys: list[PyJWK]
    refreshed_at: float
    last_refresh_attempt: float | None = None
    last_kid_refresh: float | None = None
    lock: threading.Lock = field(default_factory=threading.Lock)


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
            trusted_visa_issuers: list[BeaconVisaIssuer],
    ):
        self._issuer = issuer
        self._client_id = client_id
        self._client_secret = client_secret
        self._required_visas = required_visas
        self._trusted_visa_issuers = trusted_visa_issuers

        # Information from the well-known configuration (JSON):
        self._userinfo_endpoint: Optional[str] = None
        self._oidc_jwks: list[PyJWK] = []
        self._oidc_jwks_cache: _JwksCache | None = None
        self._trusted_visa_keys: dict[str, list[PyJWK]] = {}
        self._trusted_visa_jwks_caches: dict[str, _JwksCache] = {}

        # Request sending and retry behavior (unit: seconds)
        self._request_timeout = 3
        self._backoff_delay = 10
        self._jwks_refresh_interval = 15 * 60
        self._jwks_kid_refresh_interval = 60
        self._jwks_retry_interval = 30

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

        userinfo_endpoint = oidc_conf.get("userinfo_endpoint")
        if (
                not isinstance(userinfo_endpoint, str)
                or not userinfo_endpoint.startswith("https://")
        ):
            _log.warning(
                "OIDC configuration did not expose a valid HTTPS "
                "'userinfo_endpoint'; UserInfo passport retrieval is disabled")
            self._userinfo_endpoint = None
        else:
            self._userinfo_endpoint = userinfo_endpoint

        jwks_uri = oidc_conf.get("jwks_uri")
        if not isinstance(jwks_uri, str) or not jwks_uri.startswith("https://"):
            raise RuntimeError(
                "OIDC configuration did not expose a valid HTTPS 'jwks_uri'")

        self._oidc_jwks = self._get_jwks(jwks_uri)
        self._oidc_jwks_cache = _JwksCache(
            jwks_uri, self._oidc_jwks, time.monotonic())

        for trusted_issuer in self._trusted_visa_issuers:
            trusted_well_known_url = (
                trusted_issuer.issuer.rstrip("/")
                + "/.well-known/oidc-configuration"
            )
            trusted_oidc_conf = self._retry_json_fetch(
                trusted_well_known_url, tries=5)
            if not isinstance(trusted_oidc_conf, dict):
                _log.warning(
                    "Unable to fetch OIDC configuration from [%s]; "
                    "continuing with configured JWKS URI",
                    trusted_well_known_url,
                )
            elif trusted_oidc_conf.get("jwks_uri") != trusted_issuer.jwks_uri:
                raise RuntimeError(
                    f"JWKS URI for trusted visa issuer "
                    f"[{trusted_issuer.issuer}] does not match configured "
                    f"URI [{trusted_issuer.jwks_uri}]")

            keys = self._get_jwks(trusted_issuer.jwks_uri)
            if trusted_issuer.issuer in self._trusted_visa_keys:
                raise RuntimeError(
                    f"Duplicate trusted visa issuer "
                    f"[{trusted_issuer.issuer}]")
            self._trusted_visa_keys[trusted_issuer.issuer] = keys
            self._trusted_visa_jwks_caches[trusted_issuer.issuer] = (
                _JwksCache(trusted_issuer.jwks_uri, keys, time.monotonic()))

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

        if self._oidc_jwks_cache is None:
            raise RuntimeError("Verifier not initialized: call init() first.")

        self._get_current_jwks(self._oidc_jwks_cache)
        claims = self._validate_token(
            token,
            self._oidc_jwks_cache.keys,
            options={
                "verify_exp": True, "verify_iat": True, "verify_aud": False,
            },
            refresh_keys=lambda: self._get_current_jwks(
                self._oidc_jwks_cache, on_kid_miss=True),
        )
        if claims is None:
            _log.debug("JWT decoding failed against configured OIDC JWKS")
            self._lru.put(token, False)
            return False

        if claims.get("iss") != self._issuer:
            _log.warning("JWT issuer does not match configured OIDC issuer")
            self._lru.put(token, False)
            return False

        sub = claims.get("sub")
        if not sub:
            _log.debug("This JWT does not include 'sub': %s", token)
            self._lru.put(token, False)
            return False

        passport = self._provide_passport_data(claims, token)
        valid = self._check_passport(sub, passport)

        _log.info(
            "Validation outcome for the JWT token [sub=%s]: %s.", sub, valid,
        )

        self._lru.put(token, valid)
        return valid

    def _provide_passport_data(
            self, claims: dict, token: str,
    ) -> list[str] | None:
        scopes = claims.get("scope")
        if not isinstance(scopes, str) or not {
                "openid", "ga4gh_passport_v1",
        }.issubset(scopes.split()):
            return claims.get("ga4gh_passport_v1")

        if self._userinfo_endpoint is None:
            _log.warning(
                "Cannot retrieve GA4GH passport: UserInfo endpoint is "
                "unavailable")
            return None

        userinfo = self._retry_json_fetch(
            self._userinfo_endpoint,
            headers={"Authorization": f"Bearer {token}"},
        )
        if not isinstance(userinfo, dict):
            _log.warning("Unable to retrieve OIDC UserInfo response")
            return None

        return userinfo.get("ga4gh_passport_v1")

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

        # Select the trusted issuer from the token claims, never from its header.
        issuer = visa_claims.get("iss")
        if self._verify_visa_signature(
                subject, issuer, visa_jwt, visa_header, visa_obj):
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
            self, subject: str, issuer: str, visa_jwt: str, visa_header: dict,
            visa_obj: dict,
    ) -> bool:
        source = visa_obj.get('source')
        asserted = visa_obj.get('asserted')
        by = visa_obj.get('by')
        msg_has_visa = (
            f"Subject '{subject}' has the required visa from [{source}] issued "
            f"at {asserted} by '{by}'")

        if not isinstance(issuer, str) or not issuer:
            _log.warning(
                "%s but the visa has no valid 'iss' claim", msg_has_visa)
            return False

        visa_cache = self._trusted_visa_jwks_caches.get(issuer)
        if visa_cache is None:
            _log.warning(
                "%s but issuer [%s] is not trusted", msg_has_visa, issuer)
            return False

        self._get_current_jwks(visa_cache)
        verified_claims = self._validate_token(
            visa_jwt,
            visa_cache.keys,
            options={
                "verify_signature": True,
                "verify_iat": False,
                "verify_exp": True,
            },
            refresh_keys=lambda: self._get_current_jwks(
                visa_cache, on_kid_miss=True),
        )
        if verified_claims is not None:
            _log.info("%s.", msg_has_visa)
            return True

        _log.warning(
            "%s but JWT signature could not be verified", msg_has_visa)
        return False

    # ------------------------------------------------------
    # Helpers for fetching JWT resources
    # ------------------------------------------------------
    def _get_jwks(self, jwks_uri: str) -> list[PyJWK]:
        jwks = self._retry_json_fetch(jwks_uri, tries=5)
        if not isinstance(jwks, dict):
            raise RuntimeError(f"Unable to fetch JWKS from [{jwks_uri}]")

        keys = jwks.get("keys")
        if not isinstance(keys, list) or not keys:
            raise RuntimeError(f"JWKS from [{jwks_uri}] has no keys.")

        return [PyJWK.from_dict(key) for key in keys]

    @staticmethod
    def _validate_token(
            token: str, keys: list[PyJWK], options: dict,
            refresh_keys: Callable[[], list[PyJWK]] | None = None,
    ) -> dict | None:
        try:
            header = jwt.get_unverified_header(token)
        except PyJWTError:
            return None

        key_id = header.get("kid")
        if not key_id:
            return None
        algorithm = header.get("alg")

        def candidate_keys(current_keys: list[PyJWK]) -> list[PyJWK]:
            return [
                key for key in current_keys
                if key.algorithm_name == algorithm
                and key.key_id == key_id
            ]

        matching_keys = candidate_keys(keys)
        if (
                not matching_keys
                and refresh_keys is not None
        ):
            matching_keys = candidate_keys(refresh_keys())

        for key in matching_keys:
            try:
                return jwt.decode(
                    token,
                    key=key.key,
                    algorithms=[key.algorithm_name],
                    options=options,
                )
            except PyJWTError:
                continue
        return None

    def _get_current_jwks(
            self, cache: _JwksCache, on_kid_miss: bool = False,
    ) -> list[PyJWK]:
        now = time.monotonic()
        with cache.lock:
            if on_kid_miss:
                if (
                        cache.last_kid_refresh is not None
                        and now - cache.last_kid_refresh
                        < self._jwks_kid_refresh_interval
                ):
                    return cache.keys
                cache.last_kid_refresh = now
            elif now - cache.refreshed_at < self._jwks_refresh_interval:
                return cache.keys

            if (
                    cache.last_refresh_attempt is not None
                    and now - cache.last_refresh_attempt
                    < self._jwks_retry_interval
            ):
                return cache.keys
            cache.last_refresh_attempt = now

            try:
                fresh_keys = self._get_jwks(cache.uri)
            except (RuntimeError, PyJWTError, ValueError, TypeError) as error:
                _log.warning(
                    "Unable to refresh JWKS from [%s]; keeping cached keys: %s",
                    cache.uri, error,
                )
                return cache.keys

            cache.keys[:] = fresh_keys
            cache.refreshed_at = time.monotonic()
            return cache.keys

    def _retry_json_fetch(
            self, url: str, tries: int = 5,
            headers: dict[str, str] | None = None,
    ):
        _log.info("Fetching JSON from [%s]", url)
        for attempt in range(1, tries + 1):
            try:
                with httpx.Client(
                        timeout=self._request_timeout,
                        follow_redirects=False,
                ) as client:
                    resp = client.get(url, headers=headers)
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
