from pydantic import BaseModel
from typing import Optional, List


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


class LabelsQuery(BaseModel):
    limit: Optional[int] = 1000,
    offset: Optional[int] = 0,
    label_name_id_list: Optional[List[int]] = None,
    name_list: Optional[List[str]] = None,
    device_list: Optional[List[int | str]] = None,
    patient_id_list: Optional[List[int]] = None,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    time_units: Optional[str] = None
