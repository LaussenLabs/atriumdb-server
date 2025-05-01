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
from rest_api.app.core.auth import auth, Auth0User
from typing import Optional, Dict
from rest_api.app.core.atriumdb import atriumdb_sdk
from fastapi import APIRouter, Depends, Security, HTTPException
import rest_api.app.schemas as schemas

router = APIRouter()


@router.get("/", dependencies=[Depends(auth.implicit_scheme)], response_model=schemas.Device | Dict[int, schemas.Device])
async def search_devices(
        device_tag: Optional[str] = None,
        device_name: Optional[str] = None,
        # device_manufacturer: Optional[str],
        # model: Optional[str],
        user: Auth0User = Security(auth.get_user)):

    if device_tag is None and device_name is None:
        res = atriumdb_sdk.get_all_devices()
    else:
        res = atriumdb_sdk.search_devices(tag_match=device_tag, name_match=device_name)

    if res is None:
        raise HTTPException(status_code=404, detail="No devices Found")
    return res


@router.get("/{device_id}", dependencies=[Depends(auth.implicit_scheme)], response_model=schemas.Device)
async def get_device_info(
        device_id: int,
        user: Auth0User = Security(auth.get_user)):

    res = atriumdb_sdk.get_device_info(device_id)
    if res is None:
        raise HTTPException(status_code=404, detail=f"No device found for device_id {device_id}")
    return res


