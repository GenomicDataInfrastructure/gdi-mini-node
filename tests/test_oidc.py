import json
import time
import unittest

import jwt
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from jwt import PyJWK

from mini_node.beacon.config import BeaconVisaIssuer
from mini_node.oidc import OidcVerifier, _JwksCache


OIDC_ISSUER = "https://test.example.org/"
OIDC_JWKS_URI = "https://test.example.org/jwks"
VISA_ISSUER = "https://visa.example.org/"
VISA_JWKS_URI = "https://visa.example.org/jwks"
REQUIRED_VISA = {
    "type": "ResearcherStatus",
    "value": "https://doi.org/10.1038/s41431-018-0219-y",
}


class OidcVerifierTest(unittest.TestCase):
    def setUp(self):
        self.oidc_private_key, oidc_jwk = self._generate_jwk("oidc-key")
        self.visa_private_key, visa_jwk = self._generate_jwk("visa-key")

        self.verifier = OidcVerifier(
            issuer=OIDC_ISSUER,
            client_id="test-client",
            client_secret="test-secret",
            required_visas=[REQUIRED_VISA],
            trusted_visa_issuers=[
                BeaconVisaIssuer(
                    issuer=VISA_ISSUER,
                    jwks_uri=VISA_JWKS_URI,
                )
            ],
        )
        self.verifier._oidc_jwks = [oidc_jwk]
        self.verifier._oidc_jwks_cache = _JwksCache(
            OIDC_JWKS_URI, self.verifier._oidc_jwks, time.monotonic(),
        )
        visa_jwks = [visa_jwk]
        self.verifier._trusted_visa_keys[VISA_ISSUER] = visa_jwks
        self.verifier._trusted_visa_jwks_caches[VISA_ISSUER] = _JwksCache(
            VISA_JWKS_URI, visa_jwks, time.monotonic(),
        )

    @staticmethod
    def _generate_jwk(key_id: str) -> tuple[RSAPrivateKey, PyJWK]:
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
        )
        public_jwk = json.loads(
            jwt.algorithms.RSAAlgorithm.to_jwk(private_key.public_key())
        )
        public_jwk.update({"alg": "RS256", "kid": key_id, "use": "sig"})
        return private_key, PyJWK.from_dict(public_jwk)

    @staticmethod
    def _encode_token(
            claims: dict, private_key: RSAPrivateKey,
            key_id: str | None,
    ) -> str:
        headers = {"kid": key_id} if key_id is not None else {}
        return jwt.encode(
            claims,
            private_key,
            algorithm="RS256",
            headers=headers,
        )

    def _create_visa_token(
            self, subject: str, visa_claim: dict | None = None,
            key_id: str | None = "visa-key",
            time_skew_hours: float = 0,
    ) -> str:
        now = int(time.time() - time_skew_hours * 60 * 60)
        claims = {
            "iss": VISA_ISSUER,
            "sub": subject,
            "iat": now,
            "exp": now + 300,
            "ga4gh_visa_v1": visa_claim or REQUIRED_VISA,
        }
        return self._encode_token(claims, self.visa_private_key, key_id)

    def _create_auth_token(
            self, subject: str, visas: list[str],
            issuer: str = OIDC_ISSUER,
    ) -> str:
        now = int(time.time())
        claims = {
            "iss": issuer,
            "sub": subject,
            "iat": now,
            "exp": now + 300,
            "ga4gh_passport_v1": visas,
        }
        return self._encode_token(claims, self.oidc_private_key, "oidc-key")

    def test_verify_accepts_token_with_required_trusted_visa(self):
        subject = "test-subject"
        visa_token = self._create_visa_token(subject)
        auth_token = self._create_auth_token(subject, [visa_token])

        self.assertTrue(self.verifier.verify(auth_token))

    def test_verify_rejects_token_without_required_visa_attributes(self):
        subject = "test-subject"
        visa_token = self._create_visa_token(
            subject,
            {"type": "ResearcherStatus", "value": "https://example.org/other"},
        )
        auth_token = self._create_auth_token(subject, [visa_token])

        self.assertFalse(self.verifier.verify(auth_token))

    def test_verify_rejects_visa_token_without_kid(self):
        subject = "test-subject"
        visa_token = self._create_visa_token(subject, key_id=None)
        auth_token = self._create_auth_token(subject, [visa_token])

        self.assertFalse(self.verifier.verify(auth_token))

    def test_verify_rejects_empty_visa_array(self):
        auth_token = self._create_auth_token("test-subject", [])

        self.assertFalse(self.verifier.verify(auth_token))

    def test_verify_rejects_token_with_different_oidc_issuer(self):
        subject = "test-subject"
        visa_token = self._create_visa_token(subject)
        auth_token = self._create_auth_token(
            subject, [visa_token], issuer="https://other.example.org/",
        )

        self.assertFalse(self.verifier.verify(auth_token))

    def test_verify_rejects_expired_visa_token(self):
        subject = "test-subject"
        visa_token = self._create_visa_token(
            subject, time_skew_hours=1,
        )
        auth_token = self._create_auth_token(subject, [visa_token])

        self.assertFalse(self.verifier.verify(auth_token))


if __name__ == "__main__":
    unittest.main()
