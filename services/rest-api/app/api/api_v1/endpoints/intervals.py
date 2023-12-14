from app.api.api_v1.endpoints.query import validate_base_params, get_measure_data, convert_time_to_nano
from app.core.auth import auth, Auth0User
from app.core.database import database
from app.core.siri import siri
from app.core.atriumdb import atriumdb_sdk
from app.core.config import settings
from typing import Optional, List
from fastapi import APIRouter, Depends, Security, HTTPException
import numpy as np
import time

router = APIRouter()


@router.get("", response_model=List[List[int]], dependencies=[Depends(auth.implicit_scheme)])
async def get_intervals(

        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        device_id: Optional[int] = None,
        measure_id: Optional[int] = None,

        device_tag: Optional[str] = None,
        measure_tag: Optional[str] = None,
        measure_units: Optional[str] = None,
        freq: Optional[int | float] = None,
        freq_units: Optional[str] = None,

        patient_id: Optional[int] = None,
        mrn: Optional[str] = None,
        time_precision: Optional[str] = None,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
        user: Auth0User = Security(auth.get_user)
):
    skip = 0 if skip is None else skip
    # BASE VALIDATION
    await validate_base_params(measure_id, measure_tag, freq, measure_units, device_id, device_tag, patient_id, mrn,
                               start_time, end_time)

    # GET MEASURE DATA
    start_time, end_time = await convert_time_to_nano(start_time, end_time, time_precision)

    freq_nhz, measure_id, _, _ = await get_measure_data(measure_id, measure_tag, freq, measure_units, freq_units)

    num_sources = 0

    for source in [device_id, device_tag, patient_id, mrn]:
        if source is not None:
            num_sources += 1

    if num_sources != 1:
        raise HTTPException(status_code=400,
                            detail="Please only specify one of [device_id, device_tag, patient_id, mrn]")

    if device_tag is not None:
        device_id = atriumdb_sdk.get_device_id(device_tag=device_tag)
        if device_id is None:
            raise HTTPException(status_code=400,
                                detail=f"device_tag: {device_tag} not found.")

    if device_id is not None:
        if atriumdb_sdk.get_device_info(device_id) is None:
            raise HTTPException(status_code=400,
                                detail=f"device_id: {device_id} not found.")

        result = atriumdb_sdk.get_interval_array(
            measure_id, device_id=device_id, patient_id=None, gap_tolerance_nano=None,
            start=start_time, end=end_time).tolist()

        limit = len(result) if limit is None else limit
        return result[skip:skip + limit]

    if mrn is not None:
        mrn_map = atriumdb_sdk.get_mrn_to_patient_id_map(mrn_list=[mrn])
        if len(mrn_map) == 0:
            raise HTTPException(status_code=400,
                                detail=f"mrn: {mrn} not found.")
        patient_id = mrn_map[mrn]

    if patient_id is not None:
        result = atriumdb_sdk.get_interval_array(
            measure_id, device_id=None, patient_id=patient_id, gap_tolerance_nano=None,
            start=start_time, end=end_time).tolist()

        limit = len(result) if limit is None else limit
        return result[skip:skip + limit]
