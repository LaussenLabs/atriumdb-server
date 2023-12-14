from typing import Optional
from pydantic import BaseModel


class Encounter(BaseModel):
    id: int
    patient_id: int
    bed_id: int
    start_time: int
    end_time: Optional[int] = None
    visit_number: Optional[str] = None
    last_updated: Optional[int] = None

