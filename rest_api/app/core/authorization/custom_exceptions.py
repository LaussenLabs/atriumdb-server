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
from fastapi import WebSocketException, status


class BadCredentialsException(WebSocketException):
    def __init__(self):
        super().__init__(code=status.WS_1008_POLICY_VIOLATION, reason="Bad credentials")


class RequiresAuthenticationException(WebSocketException):
    def __init__(self):
        super().__init__(code=status.WS_1008_POLICY_VIOLATION, reason="Requires authentication")


class UnableCredentialsException(WebSocketException):
    def __init__(self):
        super().__init__(code=status.WS_1011_INTERNAL_ERROR, reason="Unable to verify credentials")