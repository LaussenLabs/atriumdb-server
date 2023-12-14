import asyncio
from walwriter.config import config
from siridb.connector import SiriDBClient


async def example(siri):
    await siri.connect()

    try:
        # query
        resp = await siri.query('select * from "wave-1-1"')
        print(resp)

        resp = await siri.query('select * from "metric-1-1"')
        print(resp)

    finally:
        # Close all SiriDB connections.
        siri.close()

siri = SiriDBClient(
    username=config.siridb['username'],
    password=config.siridb['password'],
    dbname=config.siridb['db_name'],
    hostlist=[(config.siridb['host'], config.siridb['port'])],
    keepalive=True)

loop = asyncio.get_event_loop()
loop.run_until_complete(example(siri))
