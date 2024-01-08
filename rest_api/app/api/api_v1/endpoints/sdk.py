from fastapi import APIRouter, Depends, Security, Response, HTTPException
from rest_api.app.core.auth import auth, Auth0User
from rest_api.app.core.atriumdb import atriumdb_sdk
from pathlib import Path
from typing import Optional

router = APIRouter()


@router.get("/blocks", dependencies=[Depends(auth.implicit_scheme)])
async def get_blocks(
        start_time: int,
        end_time: int,
        measure_id: int,
        patient_id: Optional[int] = None,
        mrn: Optional[str] = None,
        device_id: Optional[int] = None,
        user: Auth0User = Security(auth.get_user)):
    # BASE VALIDATION
    if measure_id is None:
        raise HTTPException(status_code=400, detail="A measure_id must be specified")
    if sum(x is not None for x in (device_id, patient_id, mrn)) != 1:
        raise HTTPException(status_code=400, detail="Exactly one of device_id, patient_id, or mrn must be specified")
    if start_time > end_time:
        raise HTTPException(status_code=400, detail="The start time must be lower than the end time")

    if device_id is not None:
        device_info = atriumdb_sdk.get_device_info(device_id=device_id)

        # If the device_id doesn't exist in atriumdb raise an error
        if device_info is None:
            raise HTTPException(status_code=400, detail=f"Cannot find device_id={device_id} in AtriumDB")

    if patient_id is not None:
        patient_id_dict = atriumdb_sdk.get_patient_id_to_mrn_map([patient_id])
        if patient_id not in patient_id_dict:
            raise HTTPException(status_code=400, detail=f"Cannot find patient_id={patient_id} in AtriumDB.")

    if mrn is not None:
        mrn_patient_id_map = atriumdb_sdk.get_mrn_to_patient_id_map([int(mrn)])
        if mrn not in mrn_patient_id_map:
            raise HTTPException(status_code=400, detail=f"Cannot find mrn={mrn} in AtriumDB.")
        patient_id = mrn_patient_id_map[mrn]

    block_info = []
    block_id_list = atriumdb_sdk.sql_handler.select_blocks(
        measure_id, start_time_n=start_time, end_time_n=end_time,
        device_id=device_id, patient_id=patient_id)

    for row in block_id_list:
        block_info.append({'id': row[0],
                           'start_time_n': row[6],
                           'end_time_n': row[7],
                           'num_values': row[8],
                           'num_bytes': row[5]})
    return block_info


@router.get("/blocks/{block_id}", dependencies=[Depends(auth.implicit_scheme)])
async def get_block(
        block_id: int,
        user: Auth0User = Security(auth.get_user)):
    # Get block_info
    block_info = atriumdb_sdk.sql_handler.select_block(block_id=block_id)

    if block_info is None:
        raise HTTPException(status_code=404, detail=f"Cannot find block_id={block_id} in AtriumDB.")

    file_id_list = [block_info[3]]
    filename = atriumdb_sdk.get_filename_dict(file_id_list)[block_info[3]]

    if not Path(filename).is_file():
        filename = atriumdb_sdk.file_api.to_abs_path(filename, block_info[1], block_info[2])

    with open(filename, 'rb') as file:
        file.seek(block_info[4])
        return Response(file.read(block_info[5]))
