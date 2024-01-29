from rest_api.app.core.auth import auth, Auth0User
from typing import Optional, List, Union, Dict
from rest_api.app.core.atriumdb import atriumdb_sdk
from fastapi import APIRouter, Depends, Security, HTTPException
import rest_api.app.schemas as schemas

router = APIRouter()


# @router.get("/", dependencies=[Depends(auth.implicit_scheme)], response_model=Dict[int, schemas.Label])
@router.get("/", response_model=List[schemas.Label])
async def search_labels(
        limit: int = 1000,
        offset: int = 0,
        label_name_id_list: Optional[List[int]] = None,
        name_list: Optional[List[str]] = None,
        device_list: Optional[List[Union[int, str]]] = None,
        patient_id_list: Optional[List[int]] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        time_units: Optional[str] = None,):
        #user: Auth0User = Security(auth.get_user)):

    # make sure they arnt asking for too much data at a time
    if limit > 1000:
        raise HTTPException(status_code=400, detail="Limits of greater than 1000 are not allowed.")

    if label_name_id_list and name_list:
        raise HTTPException(status_code=400, detail="Only one of label_name_id_list or name_list should be provided.")

    if device_list and patient_id_list:
        raise HTTPException(status_code=400, detail="Only one of device_list or patient_id_list should be provided.")

    try:
        labels = atriumdb_sdk.get_labels(
            label_name_id_list=label_name_id_list,
            name_list=name_list,
            device_list=device_list,
            patient_id_list=patient_id_list,
            start_time=start_time,
            end_time=end_time,
            time_units=time_units,
            limit=limit, offset=offset
        )
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))

    return labels


# @router.get("/names", dependencies=[Depends(auth.implicit_scheme)], response_model=Dict[int, str])
@router.get("/names", response_model=Dict[int, schemas.LabelName])
async def get_all_label_names(limit: int = 1000, offset: int = 0):#, user: Auth0User = Security(auth.get_user)):
    return atriumdb_sdk.get_all_label_names(limit=limit, offset=offset)