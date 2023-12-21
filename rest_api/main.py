from app.core.config import settings
from app.api.api_v1.api import api_router
from app.core.database import database
from app.core.siri import siri
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

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
