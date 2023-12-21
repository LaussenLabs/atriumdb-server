from app.core.auth import auth, Auth0User
from typing import Optional, Dict
from app.core.atriumdb import atriumdb_sdk
from fastapi import APIRouter, Depends, Security, HTTPException
import app.schemas as schemas

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


