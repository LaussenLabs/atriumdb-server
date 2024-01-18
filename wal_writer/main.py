import os 
import aio_pika
from aio_pika.abc import AbstractIncomingMessage
import ujson
import asyncio
from siridb.connector import SiriDBClient
from walwriter.siridb_admin_tool import SiriDBAdmin
import logging
from walwriter.wal_file_manager import WALFileManager
import ssl
from atriumdb import AtriumSDK
from walwriter.config import config
from helpers.metrics import (get_metric,
                             WALWRITER_PROCESSED_MESSAGE,
                             WALWRITER_MESSAGE_WRITE_DURATION,
                             WALWRITER_MESSAGE_SIRI_DURATION,
                             WALWRITER_ERRORS,
                             WALWRITER_NO_SIRI_CONNECTION,
                             WALWRITER_PROCESSED_MESSAGE_WAVEFORMS,
                             WALWRITER_PROCESSED_MESSAGE_METRICS,
                             )
from time import time

# set up logging
log_level = {"debug": logging.DEBUG, "info": logging.INFO, "warning": logging.WARNING, "error": logging.ERROR,
             "critical": logging.CRITICAL}
logging.basicConfig(level=log_level[config.loglevel.lower()])
_LOGGER = logging.getLogger(__name__)

# start wal file manager which will actually write the wal files to disk
wal = WALFileManager(path=config.svc_wal_writer['wal_folder_path'], 
                     file_length_time=config.svc_wal_writer['file_length_time'],
                     idle_timeout=config.svc_wal_writer['idle_timeout'],
                     gc_schedule_min=config.svc_wal_writer['gc_schedule_min'],
                     flush_max_points=config.svc_wal_writer['flush_max_points'],
                     flush_max_seconds=config.svc_wal_writer['flush_max_seconds'])

if config.svc_wal_writer['create_dataset']:
    AtriumSDK.create_dataset(dataset_location=config.dataset_location, database_type=config.svc_wal_writer['metadb_connection']['type'],
                             connection_params=config.CONNECTION_PARAMS, overwrite='ignore')
# Instantiate atriumDB sdk object
atrium_sdk = AtriumSDK(dataset_location=config.dataset_location, metadata_connection_type=config.svc_wal_writer['metadb_connection']['type'],
                       connection_params=config.CONNECTION_PARAMS)


def convert(x):
    a = float(x)
    if a.is_integer():
        return int(a)
    else:
        return float(a)


async def on_message(message: AbstractIncomingMessage):
    async with message.process(ignore_processed=True):
        processed_counter = get_metric(WALWRITER_PROCESSED_MESSAGE)
        message_siri_duration = get_metric(WALWRITER_MESSAGE_SIRI_DURATION)
        message_write_duration = get_metric(WALWRITER_MESSAGE_WRITE_DURATION)
        exception_counter = get_metric(WALWRITER_ERRORS)
        siri_no_connection_counter = get_metric(WALWRITER_NO_SIRI_CONNECTION)
        processed_waveforms_counter = get_metric(WALWRITER_PROCESSED_MESSAGE_WAVEFORMS)
        processed_metrics_counter = get_metric(WALWRITER_PROCESSED_MESSAGE_METRICS)

        # attempt to parse the message into json dictionary
        try:
            data = ujson.loads(message.body)
        except Exception:
            await message.nack()
            _LOGGER.error("Error parsing json nacking message", exc_info=True)
            exception_counter.add(1)
            return

        # once the message is parsed pass the message arguments to the wal writer, so it can make the wal file

        # if alarm drop msg since we don't need alarm messages for now
        if data['type'] == "alm":
            await message.ack()
            return
        # if the message is a waveform
        elif data['type'] == "wav" or data['type'] == "met":

            # If the measure id doesn't exist input it
            measure_id = atrium_sdk.get_measure_id(measure_tag=data['mname'], freq=int(data['freq'] * (10 ** 9)), units=data['uom'])
            if measure_id is None:
                atrium_sdk.insert_measure(measure_tag=data['mname'], freq=int(data['freq'] * (10 ** 9)), units=data['uom'])
                measure_id = atrium_sdk.get_measure_id(measure_tag=data['mname'], freq=int(data['freq'] * (10 ** 9)), units=data['uom'])
                if measure_id is None:
                    raise RuntimeError("Inserting a new measure into AtriumDB failed")

            # If the device id doesn't exist input it
            device_id = atrium_sdk.get_device_id(device_tag=str(data['devid']))
            if device_id is None:
                atrium_sdk.insert_device(device_tag=str(data['devid']))
                device_id = atrium_sdk.get_device_id(device_tag=str(data['devid']))
                if device_id is None:
                    raise RuntimeError("Inserting a new device into AtriumDB failed")
        try:
            start_time = time()
            if data['type'] == "wav":
                wal.write(device_name=str(data['devid']), server_time_ns=data['systime'], msg_type=data['type'],
                          measure_name=data['mname'], data_time_ns=data['mtime'], measure_units=data['uom'],
                          freq=data['freq'], data=data['val'], meta_data=data['srcmeta'])
                processed_waveforms_counter.add(1)
            # if the message is a metric message it won't contain the metadata field
            elif data['type'] == "met":
                wal.write(device_name=str(data['devid']), server_time_ns=data['systime'], msg_type=data['type'],
                          measure_name=data['mname'], data_time_ns=data['mtime'], measure_units=data['uom'],
                          freq=data['freq'], data=data['val'])
                processed_metrics_counter.add(1)
            message_write_duration.record((time() - start_time) * 1_000_000.00)
        except:
            await message.nack()
            _LOGGER.error("Error nacking message", exc_info=True)
            exception_counter.add(1)
            return

        # siri db stuff
        if config.svc_wal_writer['enable_siri']:
            start_time_met = time()
            start_time = int(data['mtime'])
            freq = convert(data['freq'])

            if data["type"] == "wav":
                sample_time = int((10 ** 9) // freq)
                values = data["val"].split("^")
                name = "wave-{}-{}".format(str(device_id), str(measure_id))
                tuples = [[int(start_time + (sample_time * i)), convert(value)] for i, value in enumerate(values)]

                if siri.connected:
                    await siri.insert({name: tuples})
                else:
                    siri_no_connection_counter.add(1)

            elif data["type"] == "met":
                name = "metric-{}-{}".format(str(device_id), str(measure_id))

                if siri.connected:
                    await siri.insert({name: [[int(start_time), convert(data["val"])]]})
                else:
                    siri_no_connection_counter.add(1)

            message_siri_duration.record((time() - start_time_met) * 1_000_000.00)

        await message.ack()
        processed_counter.add(1)


async def start_wal_writer():
    global siri
    global wal
    global atrium_sdk

    # set up connection to rabbitMQ
    if config.rabbitmq['encrypt']:
        connection = await aio_pika.connect_robust(
            host=config.rabbitmq['host'],
            port=config.rabbitmq['port'],
            login=config.rabbitmq['username'],
            password=config.rabbitmq['password'],
            ssl=True,
            ssl_context=ssl.create_default_context(cafile=config.rabbitmq['certificate_path']))
    else:
        connection = await aio_pika.connect_robust(
            host=config.rabbitmq['host'],
            port=config.rabbitmq['port'],
            login=config.rabbitmq['username'],
            password=config.rabbitmq['password'])

    if config.svc_wal_writer['enable_siri']:
        # create default database if it was specified in the config
        if config.siridb['create_default_database']:
            # start siridb admin interface
            admin = SiriDBAdmin(host=config.siridb['hosts'][0][0], port=config.siridb['admin_port'])

            # if there is no siri database create a default one (every database is created with default user=iris,
            # pass=siri used ==2 because the get returns a string "[]" so length is 2 if there are no databases
            if len(admin.get_databases()) == 2:
                admin.new_database(db_name=config.siridb['db_name'])
                _LOGGER.info("No SiriDB database detected creating one called {}".format(config.siridb['db_name']))

        siri = SiriDBClient(
            username=config.siridb['username'],
            password=config.siridb['password'],
            dbname=config.siridb['db_name'],
            hostlist=config.siridb['hosts'],  # Multiple connections are supported
            keepalive=True,
            max_wait_retry=config.siridb['max_wait_retry'])

        # if siri cant connect after n seconds throw a timeout error that will stop the code and container will restart
        await siri.connect(timeout=config.siridb["connection_timeout"])

        # Set siridb timezone if it hasn't been set yet
        resp = await siri.query("show timezone")
        if resp['data'][0]['value'] != config.timezone:
            resp = await siri.query("alter database set timezone '{}'".format(config.timezone))
            _LOGGER.info(resp)

        # Set siridb data expiration time
        resp = await siri.query("alter database set expiration_num {} set ignore_threshold true".format(config.siridb['data_expiration_time']))
        _LOGGER.info(resp)

    async with connection:
        try:
            # Creating channel
            channel = await connection.channel(publisher_confirms=False)

            # Maximum message count which will be processing at the same time.
            await channel.set_qos(prefetch_count=config.svc_wal_writer['prefetch_count'])

            # Declaring queues
            queue = await channel.declare_queue(name=config.svc_wal_writer['inbound_queue'], passive=True, durable=True)
            _LOGGER.info("Starting Ingest")

            # start listening for messages
            await queue.consume(on_message)

            _LOGGER.info("Waiting for CMF messages...")
            await asyncio.Future()
        finally:
            if config.svc_wal_writer['enable_siri']:
                siri.close()


if __name__ == "__main__":
    asyncio.run(start_wal_writer())
