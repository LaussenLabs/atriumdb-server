from typing import List
from pydantic import BaseModel


class Authentication(BaseModel):
    auth0_tenant: str
    auth0_audience: str
    auth0_client_id: str
    algorithms: List[str]
