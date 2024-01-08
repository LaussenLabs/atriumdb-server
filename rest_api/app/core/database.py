import databases
from rest_api.app.core.config import settings


database = databases.Database(settings.MARIADB_URI,
                              min_size=settings.MARIADB_POOL_MIN,
                              max_size=settings.MARIADB_POOL_MAX,
                              pool_recycle=settings.MARIADB_POOL_RECYCLE)

