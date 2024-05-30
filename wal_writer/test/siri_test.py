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