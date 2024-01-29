from pydantic import BaseModel
from typing import Optional


class LabelName(BaseModel):
    id: int
    name: str


class Label(BaseModel):
    label_id: int
    label_name_id: int
    label_name: str
    device_id: int
    device_tag: str
    patient_id: Optional[int] = None
    mrn: Optional[int] = None
    start_time_n: int
    end_time_n: int
    label_source_id: Optional[int] = None
    label_source: Optional[str] = None
