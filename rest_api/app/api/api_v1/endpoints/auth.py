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
from fastapi import APIRouter
from rest_api.app.core.config import settings
import rest_api.app.schemas as schemas
router = APIRouter()


@router.get("/cli/code", response_model=schemas.Authentication)
async def get_auth_token():
    return {"auth0_tenant": settings.AUTH0_TENANT,
            "auth0_audience": settings.AUTH0_AUDIENCE,
            "auth0_client_id": settings.AUTH0_CLIENT_ID,
            "algorithms": settings.ALGORITHMS}
