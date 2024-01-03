from fastapi import APIRouter
from rest_api.app.core.config import settings
import rest_api.app.schemas as schemas
router = APIRouter()


@router.get("/cli/code", response_model=schemas.Authentication)
async def get_auth_token():
    return {"auth0_tenant": settings.AUTH0_TENANT,
            "auth0_audience": settings.AUTH0_AUDIENCE,
            "auth0_client_id": settings.AUTH0_CLIENT_ID,
            "algorithms": settings.ALGORITHMS}
