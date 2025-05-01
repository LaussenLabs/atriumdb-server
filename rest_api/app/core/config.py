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
from typing import Any, Dict, Optional, Set, List
from pydantic import BaseSettings, AnyUrl, validator


class Settings(BaseSettings):
    API_TITLE: str = "AtriumDB API"
    API_ROOT_PATH: str = ""
    AUTH0_TENANT: str
    AUTH0_AUDIENCE: str
    AUTH0_CLIENT_ID: str
    ALGORITHMS: List[str]
    LOGLEVEL: str

    MARIADB_SERVER: str
    MARIADB_PORT: int = 3306
    MARIADB_API_USER: str
    MARIADB_API_PASSWORD: str
    MARIADB_DB: str
    MARIADB_POOL_MIN: int = 5
    MARIADB_POOL_MAX: int = 20
    MARIADB_POOL_RECYCLE: int = 600
    MARIADB_URI: Optional[AnyUrl] = None

    @validator("MARIADB_URI", pre=True)
    def assemble_db_connection(cls, v: Optional[str], values: Dict[str, Any]) -> Any:
        if isinstance(v, str):
            return v
        return AnyUrl.build(
            scheme="mysql",
            user=values.get("MARIADB_API_USER"),
            password=values.get("MARIADB_API_PASSWORD"),
            host=values.get("MARIADB_SERVER"),
            port=str(values.get("MARIADB_PORT")),
            path=f"/{values.get('MARIADB_DB') or ''}",
        )

    SIRIDB_HOSTS: Set[str] = set()
    SIRIDB_PORT: int = 9000
    SIRIDB_API_USER: str = "iris"
    SIRIDB_API_PASSWORD: str
    SIRIDB_DB: str = "testdb"

    INFLUX_API_TOKEN: str = ""
    INFLUX_ORG: str = ""
    INFLUX_BUCKET: str = ""
    INFLUX_URL: str = ""

    ATRIUMDB_DB_CONNECTION_PARAMS: Dict[str, Any] = None

    @validator("ATRIUMDB_DB_CONNECTION_PARAMS", pre=True)
    def assemble_atriumdb_db_connection(cls, v: Optional[str], values: Dict[str, Any]) -> Any:
        return {
            'host': values.get("MARIADB_SERVER"),
            'user': values.get("MARIADB_API_USER"),
            'password': values.get("MARIADB_API_PASSWORD"),
            'database': values.get('MARIADB_DB'),
            'port': values.get("MARIADB_PORT")
        }

    ATRIUMDB_DATA_PATH: str

    # Maximum amount of time to request (seconds)
    METRICS_MAX_TIME: int = 86400
    # usually 30
    WAVES_MAX_TIME: int = 120
    # Crossover time between SiriDB and AtriumDB (hours)
    XOVER_TIME: int = 720  # 30 days
    # the max size in bytes that the api will pull from AtriumDB before sending and moving onto the next fragment
    MAX_WEBSOCKET_FRAGMENT_SIZE: int = 10_000_000

    class Config:
        secrets_dir = '../'
        case_sensitive = True
        env_file = ".env"


settings = Settings()
