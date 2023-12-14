from fastapi_auth0 import Auth0, Auth0User
from app.core.config import settings
auth = Auth0(domain=settings.AUTH0_TENANT, api_audience=settings.AUTH0_AUDIENCE)
