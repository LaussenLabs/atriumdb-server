from typing import List
from fastapi import APIRouter, Depends, Security
from app.core.auth import auth, Auth0User
from app.core.database import database
import app.schemas as schemas
import app.models as models

router = APIRouter()


# grabs the current census which just the current patients
@router.get("/", dependencies=[Depends(auth.implicit_scheme)], response_model=List[schemas.Census])
# async def get_census():
async def get_census(
        user: Auth0User = Security(auth.get_user)):
    return await database.fetch_all(models.Census.select())
