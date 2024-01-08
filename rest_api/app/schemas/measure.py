from typing import Optional
from pydantic import BaseModel


class Measure(BaseModel):
    id: int
    tag: str
    name: Optional[str] = None
    freq_nhz: int
    code: Optional[str] = None
    unit: str
    unit_label: Optional[str] = None
    unit_code: Optional[str] = None
    source_id: Optional[int] = None
