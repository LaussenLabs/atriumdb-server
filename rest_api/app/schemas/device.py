from typing import Optional
from pydantic import BaseModel


class Device(BaseModel):
    id: int
    tag: str
    name: Optional[str] = None
    manufacturer: Optional[str] = None
    model: Optional[str] = None
    type: Optional[str] = None
    bed_id: Optional[int] = None
    source_id: Optional[int] = None
