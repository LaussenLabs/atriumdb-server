#
# AtriumDB is a timeseries database software designed to best handle the unique
# features and challenges that arise from clinical waveform data.
#
# Copyright (c) 2025 The Hospital for Sick Children.
#
# This file is part of AtriumDB 
# (see atriumdb.io).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
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
