from rest_api.app.core.auth import auth, Auth0User
from typing import Optional, Dict
from rest_api.app.core.atriumdb import atriumdb_sdk
from fastapi import APIRouter, Depends, Security, HTTPException
import rest_api.app.schemas as schemas

router = APIRouter()


@router.get("/", dependencies=[Depends(auth.implicit_scheme)], response_model=Dict[int, schemas.Measure])
async def search_measures(
        measure_tag: Optional[str] = None,
        measure_name: Optional[str] = None,
        unit: Optional[str] = None,
        freq: Optional[int | float] = None,
        freq_units: Optional[str] = None,
        user: Auth0User = Security(auth.get_user)):

    if measure_tag is None and measure_name is None and unit is None and freq is None:
        return atriumdb_sdk.get_all_measures()
    else:
        res = atriumdb_sdk.search_measures(tag_match=measure_tag, freq=freq, unit=unit, name_match=measure_name,
                                           freq_units=freq_units)

    if res is None:
        raise HTTPException(status_code=404, detail="No Measures Found")
    return res


@router.get("/{measure_id}", dependencies=[Depends(auth.implicit_scheme)], response_model=schemas.Measure)
async def get_measure_info(
        measure_id: int,
        user: Auth0User = Security(auth.get_user)):

    res = atriumdb_sdk.get_measure_info(measure_id)
    if res is None:
        raise HTTPException(status_code=404, detail=f"No Measures Found for {measure_id}")
    return res
