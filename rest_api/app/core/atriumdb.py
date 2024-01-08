from atriumdb import AtriumSDK
from rest_api.app.core.config import settings

atriumdb_sdk = AtriumSDK(dataset_location=settings.ATRIUMDB_DATA_PATH, metadata_connection_type='mariadb', connection_params=settings.ATRIUMDB_DB_CONNECTION_PARAMS)
