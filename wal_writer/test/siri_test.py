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
import asyncio
from wal_writer.walwriter.config import config
from siridb.connector import SiriDBClient

siri = SiriDBClient(
    username=config.siridb['username'],
    password=config.siridb['password'],
    dbname=config.siridb['db_name'],
    hostlist=config.siridb['hosts'],  # Multiple connections are supported
    keepalive=True,
    max_wait_retry=config.siridb['max_wait_retry'])


async def example(siri):
    await siri.connect(timeout=config.siridb["connection_timeout"])

    try:
        # query
        resp = await siri.query('select * from "wave-1-1"')
        print(resp)

        resp = await siri.query('select * from "metric-1-1"')
        print(resp)

    finally:
        # Close all SiriDB connections.
        siri.close()

if __name__ == "__main__":

    asyncio.run(example(siri))

    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(example(siri))