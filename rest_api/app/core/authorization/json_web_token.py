import jwt
from dataclasses import dataclass
from rest_api.app.core.config import settings
from rest_api.app.core.authorization.custom_exceptions import BadCredentialsException, UnableCredentialsException


@dataclass
class JsonWebToken:
    """Perform JSON Web Token (JWT) validation using PyJWT"""

    jwt_access_token: str
    auth0_issuer_url: str = f"https://{settings.AUTH0_TENANT}/"
    auth0_audience: str = settings.AUTH0_AUDIENCE
    algorithm: str = settings.ALGORITHMS[0]
    jwks_uri: str = f"https://{settings.AUTH0_TENANT}/.well-known/jwks.json"

    def validate(self):
        try:
            jwks_client = jwt.PyJWKClient(self.jwks_uri)
            jwt_signing_key = jwks_client.get_signing_key_from_jwt(
                self.jwt_access_token
            ).key
            payload = jwt.decode(
                self.jwt_access_token,
                jwt_signing_key,
                algorithms=self.algorithm,
                audience=self.auth0_audience,
                issuer=self.auth0_issuer_url,
            )
        except jwt.exceptions.PyJWKClientError:
            raise UnableCredentialsException
        except jwt.exceptions.InvalidTokenError:
            raise BadCredentialsException
        return payload