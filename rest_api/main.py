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
from rest_api.app.core.config import settings
from rest_api.app.api.api_v1.api import api_router
from rest_api.app.core.database import database
from rest_api.app.core.siri import siri
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
# import uvicorn

# set up logging
log_level = {"debug": logging.DEBUG, "info": logging.INFO, "warning": logging.WARNING, "error": logging.ERROR,
             "critical": logging.CRITICAL}
logging.basicConfig(level=log_level[settings.LOGLEVEL.lower()])
_LOGGER = logging.getLogger(__name__)

app = FastAPI(title=settings.API_TITLE, root_path=settings.API_ROOT_PATH)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup():
    await database.connect()
    await siri.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()
    siri.close()

FastAPIInstrumentor.instrument_app(app)
app.include_router(api_router, prefix="/v1")

# # for debugging
# if __name__ == "__main__":
#     uvicorn.run(app)