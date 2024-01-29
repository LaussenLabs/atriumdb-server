from typing import List, Dict
from fastapi import APIRouter, Depends, Security, HTTPException
from rest_api.app.core.auth import auth, Auth0User
from rest_api.app.core.database import database
from rest_api.app.api.api_v1.endpoints.query import get_device_list
import rest_api.app.schemas as schemas
import rest_api.app.models as models
from rest_api.app.core.atriumdb import atriumdb_sdk

router = APIRouter()


@router.get("/", dependencies=[Depends(auth.implicit_scheme)], response_model=Dict[int, schemas.Patient])
async def get_patients(
        skip: int = 0,
        limit: int = 100,
        current_user: Auth0User = Security(auth.get_user)):
    return atriumdb_sdk.get_all_patients(skip=skip, limit=limit)


@router.get("/{id}", dependencies=[Depends(auth.implicit_scheme)], response_model=schemas.Patient)
async def get_patient(
        id: str,
        user: Auth0User = Security(auth.get_user)):
    # check to make sure the user is using proper identifier format
    if "id|" not in id and "mrn|" not in id:
        raise HTTPException(status_code=400, detail="Patient id or mrn malformed. Must be of the structure 'id|12345' if searching by patient id or 'mrn|1234567' if searching by mrn")

    # split on pipe character to see if the prefix is "id" or "mrn" and query accordingly
    split = id.split('|')
    if split[0] == 'mrn':
        res = await database.fetch_one(models.Patient.select().where(models.Patient.c.mrn == int(split[1])))
    else:
        res = await database.fetch_one(models.Patient.select().where(models.Patient.c.id == int(split[1])))
    if res is None:
        raise HTTPException(status_code=404, detail="Patient not found")
    return res


@router.get("/{id}/encounters", dependencies=[Depends(auth.implicit_scheme)], response_model=List[schemas.Encounter])
async def get_encounters(
        id: str,
        user: Auth0User = Security(auth.get_user)):
    if "id|" not in id and "mrn|" not in id:
        raise HTTPException(status_code=400, detail="Patient id or mrn malformed. Must be of the structure 'id|12345' if searching by patient id or 'mrn|1234567' if searching by mrn")

    split = id.split('|')
    if split[0] == 'mrn':
        patient = await database.fetch_one(models.Patient.select().where(models.Patient.c.mrn == split[1]))
        res = await database.fetch_all(models.Encounter.select().where(models.Encounter.c.patient_id == patient.id))
    else:
        res = await database.fetch_all(models.Encounter.select().where(models.Encounter.c.patient_id == split[1]))
    if res is None:
        raise HTTPException(status_code=404, detail="No encounters found for patient")
    return res


@router.get("/{id}/devices", dependencies=[Depends(auth.implicit_scheme)])
async def get_devices_for_patient_id_or_mrn(
        id: str,
        start_time: int,
        end_time: int,
        user: Auth0User = Security(auth.get_user)):
    if "id|" not in id and "mrn|" not in id:
        raise HTTPException(status_code=400, detail="Patient id or mrn malformed. Must be of the structure 'id|12345' if searching by patient id or 'mrn|1234567' if searching by mrn")

    split = id.split('|')
    if split[0] == 'mrn':
        res = await get_device_list(start_time=start_time, end_time=end_time, mrn=split[1])
    else:
        res = await get_device_list(start_time=start_time, end_time=end_time, patient_id=split[1])

    if res is None:
        raise HTTPException(status_code=404, detail="No devices found for patient")
    return res


