#
# AtriumDB is a timeseries database software designed to best handle the unique
# features and challenges that arise from clinical waveform data.
#
# Copyright (c) 2025 The Hospital for Sick Children.
#
# This file is part of AtriumDB 
# (see atriumdb.io).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
from fastapi import APIRouter

from rest_api.app.api.api_v1.endpoints import census, patients, query, sdk, devices, measures, health, auth, intervals, labels

api_router = APIRouter()
api_router.include_router(census.router, prefix="/census", tags=["Census"])
api_router.include_router(patients.router, prefix="/patients", tags=["Patients"])
api_router.include_router(query.router, prefix="/query", tags=["Query"])
api_router.include_router(sdk.router, prefix="/sdk", tags=["SDK"])
api_router.include_router(devices.router, prefix="/devices", tags=["Devices"])
api_router.include_router(measures.router, prefix="/measures", tags=["Measures"])
api_router.include_router(intervals.router, prefix="/intervals", tags=["Intervals"])
api_router.include_router(labels.router, prefix="/labels", tags=["Labels"])
api_router.include_router(health.router, prefix="/health", tags=["Health"])
api_router.include_router(auth.router, prefix="/auth", tags=["Authentication"])
