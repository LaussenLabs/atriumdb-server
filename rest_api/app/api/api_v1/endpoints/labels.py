from rest_api.app.core.auth import auth, Auth0User
from typing import Optional, List, Union, Dict
from rest_api.app.core.atriumdb import atriumdb_sdk
from fastapi import APIRouter, Depends, Security, HTTPException
import rest_api.app.schemas as schemas

router = APIRouter()


# @router.get("/", dependencies=[Depends(auth.implicit_scheme)], response_model=Dict[int, schemas.Label])
@router.post("/", response_model=List[schemas.Label])
async def search_labels(body: schemas.LabelsQuery):
        #user: Auth0User = Security(auth.get_user)):

    # make sure they arnt asking for too much data at a time
    if body.limit > 1000:
        raise HTTPException(status_code=400, detail="Limits of greater than 1000 are not allowed.")

    if body.label_name_id_list and body.name_list:
        raise HTTPException(status_code=400, detail="Only one of label_name_id_list or name_list should be provided.")

    if body.device_list and body.patient_id_list:
        raise HTTPException(status_code=400, detail="Only one of device_list or patient_id_list should be provided.")

    try:
        labels = atriumdb_sdk.get_labels(
            label_name_id_list=body.label_name_id_list,
            name_list=body.name_list,
            device_list=body.device_list,
            patient_id_list=body.patient_id_list,
            start_time=body.start_time,
            end_time=body.end_time,
            time_units=body.time_units,
            limit=body.limit, offset=body.offset
        )
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))

    return labels


# @router.get("/names", dependencies=[Depends(auth.implicit_scheme)], response_model=Dict[int, str])
@router.get("/name", response_model=Dict[int, schemas.LabelName])
async def get_all_label_names(label_name_id: int = None, label_name: str = None):#, user: Auth0User = Security(auth.get_user)):

    if label_name_id and label_name:
        raise HTTPException(status_code=400, detail="Only one of label_name_id or label_name should be provided.")

    if label_name_id is not None:
        return atriumdb_sdk.get_label_name_info(label_name_id=label_name_id)
    if label_name is not None:
        return atriumdb_sdk.get_label_name_id(name=label_name)


# @router.get("/names", dependencies=[Depends(auth.implicit_scheme)], response_model=Dict[int, str])
@router.get("/names", response_model=Dict[int, schemas.LabelName])
async def get_all_label_names(limit: int = 1000, offset: int = 0):#, user: Auth0User = Security(auth.get_user)):
    return atriumdb_sdk.get_all_label_names(limit=limit, offset=offset)


# @router.get("/names", dependencies=[Depends(auth.implicit_scheme)], response_model=Dict[int, str])
@router.get("/source", response_model=Dict[int, schemas.LabelName])
async def get_all_label_names(label_source_id: int = None, label_source_name: str = None):#, user: Auth0User = Security(auth.get_user)):

    if label_source_id and label_source_name:
        raise HTTPException(status_code=400, detail="Only one of label_source_id or label_source_name should be provided.")

    if label_source_id is not None:
        return atriumdb_sdk.get_label_source_info(label_source_id=label_source_id)
    if label_source_name is not None:
        return atriumdb_sdk.get_label_source_id(name=label_source_name)

