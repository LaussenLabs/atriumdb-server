from siridb.connector import SiriDBClient
from rest_api.app.core.config import settings

hostlist = []
for host in settings.SIRIDB_HOSTS:
    hostlist.append((host, settings.SIRIDB_PORT))

siri = SiriDBClient(
    username=settings.SIRIDB_API_USER,
    password=settings.SIRIDB_API_PASSWORD,
    dbname=settings.SIRIDB_DB,
    hostlist=hostlist,  # Multiple connections are supported
    keepalive=True)
