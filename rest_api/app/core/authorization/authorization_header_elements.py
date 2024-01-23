from typing import NamedTuple

from rest_api.app.core.authorization.custom_exceptions import BadCredentialsException, RequiresAuthenticationException
from fastapi import WebSocket


class AuthorizationHeaderElements(NamedTuple):
    authorization_scheme: str
    bearer_token: str
    are_valid: bool


def get_authorization_header_elements(authorization_header: str,) -> AuthorizationHeaderElements:
    try:
        authorization_scheme, bearer_token = authorization_header.split()
    except ValueError:
        raise BadCredentialsException
    else:
        valid = authorization_scheme.lower() == "bearer" and bool(bearer_token.strip())
        return AuthorizationHeaderElements(authorization_scheme, bearer_token, valid)


def get_bearer_token(websocket: WebSocket) -> str:
    authorization_header = websocket.headers['Authorization']
    if authorization_header:
        authorization_header_elements = get_authorization_header_elements(
            authorization_header
        )
        if authorization_header_elements.are_valid:
            return authorization_header_elements.bearer_token
        else:
            raise BadCredentialsException
    else:
        raise RequiresAuthenticationException
