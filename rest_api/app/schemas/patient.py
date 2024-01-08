from typing import Optional
from pydantic import BaseModel


class Patient(BaseModel):
    id: int
    mrn: int
    gender: str
    dob: int
    first_name: Optional[str]  # needs to be optional because there's one patient in the database with no first name
    middle_name: Optional[str] = None
    last_name: str
    first_seen: Optional[int] = None
    last_updated: Optional[int] = None
    source_id: Optional[int] = None
    height: Optional[float] = None
    weight: Optional[float] = None
