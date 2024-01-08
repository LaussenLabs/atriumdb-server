from fastapi import APIRouter

from rest_api.app.api.api_v1.endpoints import census, patients, query, sdk, devices, measures, health, auth, intervals

api_router = APIRouter()
api_router.include_router(census.router, prefix="/census", tags=["Census"])
api_router.include_router(patients.router, prefix="/patients", tags=["Patients"])
api_router.include_router(query.router, prefix="/query", tags=["Query"])
api_router.include_router(sdk.router, prefix="/sdk", tags=["SDK"])
api_router.include_router(devices.router, prefix="/devices", tags=["Devices"])
api_router.include_router(measures.router, prefix="/measures", tags=["Measures"])
api_router.include_router(intervals.router, prefix="/intervals", tags=["Intervals"])
api_router.include_router(health.router, prefix="/health", tags=["Health"])
api_router.include_router(auth.router, prefix="/auth", tags=["Authentication"])
