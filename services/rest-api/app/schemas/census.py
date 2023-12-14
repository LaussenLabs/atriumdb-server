from typing import Optional
from pydantic import BaseModel


class Census(BaseModel):
    admission_start: Optional[int] = None
    unit_id: int
    unit_name: str
    bed_id: int
    bed_name: str
    patient_id: Optional[int] = None
    mrn: Optional[int] = None
    first_name: Optional[str] = None
    middle_name: Optional[str] = None
    last_name: Optional[str] = None
    gender: Optional[str] = None
    birth_date: Optional[int] = None
    height: Optional[float] = None
    weight: Optional[float] = None
