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
from typing import List
from fastapi import APIRouter, Depends, Security
from rest_api.app.core.auth import auth, Auth0User
from rest_api.app.core.database import database
import rest_api.app.schemas as schemas
import rest_api.app.models as models

router = APIRouter()


# grabs the current census which just the current patients
@router.get("/", dependencies=[Depends(auth.implicit_scheme)], response_model=List[schemas.Census])
# async def get_census():
async def get_census(
        user: Auth0User = Security(auth.get_user)):
    return await database.fetch_all(models.Census.select())
