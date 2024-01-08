# from influxdb_client import InfluxDBClient
# from app.core.config import settings
# import influxdb_client.client.util.date_utils as date_utils
# from influxdb_client.client.util.date_utils_pandas import PandasDateTimeHelper
#
# # import pandas datetime helper so influx can deal with nanoseconds
# date_utils.date_helper = PandasDateTimeHelper()
#
# client = InfluxDBClient(url=settings.INFLUX_URL, token=settings.INFLUX_API_TOKEN, org=settings.INFLUX_ORG)
# influx = client.query_api()