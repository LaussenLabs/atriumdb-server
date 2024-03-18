import pika
import time
from atriumdb import AtriumSDK
from atriumdb.sql_handler.maria.maria_handler import MariaDBHandler
from pathlib import Path
import shutil
import os
import numpy as np
import orjson
from wal_writer.walwriter.config import config
import ssl


# ********************************************************************************************************************
# ******************CAUTION THIS TEST WILL RESET THE DATABASE IN WHATEVER DATASET DIRECTORY YOU USE*******************
# ********************************************************************************************************************


# this should be set to whatever the directory is set to in the docker compose file
DATASET_DIR = "../../test_data"
db_name = 'server-test'


def test_end_to_end_wav():
    # clear database dir and drop old test table from mariadb
    reset_database(DATASET_DIR)

    sdk = AtriumSDK.create_dataset(dataset_location=DATASET_DIR, database_type=config.svc_wal_writer['metadb_connection']['type'],
                                   connection_params=config.CONNECTION_PARAMS)

    # send test data to rabbitMQ
    send_data("data/single_cmf_test_wav.txt")

    # wait 2 mins for data to be ingested
    time.sleep(120)

    devices = sdk.get_all_devices()
    # make sure the device is named correctly
    assert devices[1]['id'] == sdk.get_device_id(device_tag="97")

    measures = sdk.get_all_measures()
    # make sure there is only one measure
    assert len(measures) == 1
    # make sure the measure was inserted correctly
    assert measures[1]['id'] == sdk.get_measure_id(measure_tag="MDC_RESP", freq=62_500_000_000, units="MDC_DIM_X_OHM")

    # extract wav data from cmf messages
    wav_data = []
    time_data = []
    with open("data/single_cmf_test_wav.txt", 'r') as f:
        for line in f:
            data = orjson.loads(line)
            values = np.fromstring(data['val'], dtype=float, sep='^')
            values = ((values - data['srcmeta']["scale_b"]) / data['srcmeta']["scale_m"])  # convert to ints
            values = np.rint(values).astype(np.dtype("<i2"))
            wav_data.append(values)

            period_nano = int((10 ** 9) // data['freq'])
            time_data.append(np.arange(data['mtime'], data['mtime'] + (len(values) * period_nano), period_nano, dtype=np.int64))

    # flatten list
    data = [i for sublist in wav_data for i in sublist]
    data_time = [i for sublist in time_data for i in sublist]

    # get stored data from atriumDB using the sdk
    header, read_times, read_values = sdk.get_data(measure_id=measures[1]['id'], start_time_n=0, end_time_n=1651132415296000000, device_id=devices[1]['id'], analog=False)

    # confirm data is stored correctly
    assert np.array_equal(data, read_values)
    # confirm times are stored correctly
    assert np.array_equal(data_time, read_times)

    # confirm scaling factors are stored correctly
    assert header[0].scale_m == 0.0003907204
    assert header[0].scale_b == -0.4
    print("Waveforms test passed")


# this will test when there are no scale factors provided and when message sizes are variable
def test_end_to_end_wav_noscale():
    # clear database dir and drop old test table from mariadb
    reset_database(DATASET_DIR)

    sdk = AtriumSDK.create_dataset(dataset_location=DATASET_DIR, database_type=config.svc_wal_writer['metadb_connection']['type'],
                                   connection_params=config.CONNECTION_PARAMS)

    # send test data to rabbitMQ
    send_data("data/single_cmf_test_wav_no_scale.txt")

    # wait 2 mins for data to be ingested
    time.sleep(120)

    devices = sdk.get_all_devices()
    # make sure the device is named correctly
    assert devices[1]['id'] == sdk.get_device_id(device_tag="97")

    measures = sdk.get_all_measures()
    # make sure there is only one measure
    assert len(measures) == 1
    # make sure the measure was inserted correctly
    assert measures[1]['id'] == sdk.get_measure_id(measure_tag="MDC_RESP", freq=62_500_000_000, units="MDC_DIM_X_OHM")

    # extract wav data from cmf messages
    wav_data = []
    time_data = []
    with open("data/single_cmf_test_wav_no_scale.txt", 'r') as f:
        for line in f:
            data = orjson.loads(line)
            values = np.fromstring(data['val'], dtype=float, sep='^')
            wav_data.append(values)

            period_nano = int((10 ** 9) // data['freq'])
            time_data.append(np.arange(data['mtime'], data['mtime'] + (len(values) * period_nano), period_nano, dtype=np.int64))

    # flatten list
    data = [i for sublist in wav_data for i in sublist]
    data_time = [i for sublist in time_data for i in sublist]

    # get stored data from atriumDB using the sdk
    header, read_times, read_values = sdk.get_data(measure_id=measures[1]['id'], start_time_n=0, end_time_n=1651132415296000000, device_id=devices[1]['id'])

    # confirm data is stored correctly
    assert np.array_equal(data, read_values)

    # confirm times are stored correctly
    assert np.array_equal(data_time, read_times)

    # confirm scaling factors are stored correctly
    assert header[0].scale_m == 0
    assert header[0].scale_b == 0
    print("Waveforms with variable message sizes and no scale factors test passed")


def test_end_to_end_met():
    # clear database dir and drop old test table from mariadb
    reset_database(DATASET_DIR)

    sdk = AtriumSDK.create_dataset(dataset_location=DATASET_DIR, database_type=config.svc_wal_writer['metadb_connection']['type'],
                                   connection_params=config.CONNECTION_PARAMS)

    # send test data to rabbitMQ
    send_data("data/single_cmf_test_met.txt")

    # wait 2 mins for data to be ingested
    time.sleep(120)

    devices = sdk.get_all_devices()
    # make sure the device is named correctly
    assert devices[1]['id'] == sdk.get_device_id(device_tag="110")

    measures = sdk.get_all_measures()
    # make sure there are 4 measures
    assert len(measures) == 4
    # make sure each measure was inserted correctly
    measure_id_1 = sdk.get_measure_id(measure_tag="MDC_RESP_RATE", freq=0.9765625, freq_units="Hz", units="MDC_DIM_RESP_PER_MIN")
    assert measure_id_1 in measures
    measure_id_2 = sdk.get_measure_id(measure_tag="MDC_AWAY_RESP_RATE", freq=0.9765625, freq_units="Hz", units="MDC_DIM_RESP_PER_MIN")
    assert measure_id_2 in measures
    measure_id_3 = sdk.get_measure_id(measure_tag="MDC_AWAY_CO2_ET", freq=0.9765625, freq_units="Hz", units="MDC_DIM_MMHG")
    assert measure_id_3 in measures
    measure_id_4 = sdk.get_measure_id(measure_tag="MDC_AWAY_CO2_INSP_MIN", freq=0.9765625, freq_units="Hz", units="MDC_DIM_MMHG")
    assert measure_id_4 in measures

    # ensure the values, time and measure units were inserted correctly
    _, read_times, read_values = sdk.get_data(measure_id=measure_id_1, start_time_n=0, end_time_n=1751124948520000000, device_id=devices[1]['id'])

    assert read_values[0] == 28
    assert read_times[0] == 1651124948520000000
    assert read_values[1] == 29
    assert read_times[1] == 1651124949544000000
    assert read_values[2] == 29
    assert read_times[2] == 1651124950568000000
    assert measures[measure_id_1]['unit'] == 'MDC_DIM_RESP_PER_MIN'

    _, read_times, read_values = sdk.get_data(measure_id=measure_id_2, start_time_n=0, end_time_n=1751124948520000000, device_id=devices[1]['id'])
    assert read_values[0] == 32
    assert read_times[0] == 1651124948520000000
    assert measures[measure_id_2]['unit'] == 'MDC_DIM_RESP_PER_MIN'

    _, read_times, read_values = sdk.get_data(measure_id=measure_id_3, start_time_n=0, end_time_n=1751124948520000000, device_id=devices[1]['id'])
    assert read_values[0] == 27
    assert read_times[0] == 1651124948520000000
    assert measures[measure_id_3]['unit'] == 'MDC_DIM_MMHG'

    _, read_times, read_values = sdk.get_data(measure_id=measure_id_4, start_time_n=0, end_time_n=1751124948520000000, device_id=devices[1]['id'])
    assert read_values[0] == 0
    assert read_times[0] == 1651124948520000000
    assert measures[measure_id_4]['unit'] == 'MDC_DIM_MMHG'
    print("Metrics test passed")


def test_end_to_end_aperiodic_met():
    # clear database dir and drop old test table from mariadb
    reset_database(DATASET_DIR)

    sdk = AtriumSDK.create_dataset(dataset_location=DATASET_DIR, database_type=config.svc_wal_writer['metadb_connection']['type'],
                                   connection_params=config.CONNECTION_PARAMS)

    # send test data to rabbitMQ
    send_data("data/single_cmf_test_aperiodic_met.txt")

    # wait 2 mins for data to be ingested
    time.sleep(120)

    devices = sdk.get_all_devices()
    # make sure the device is named correctly
    assert devices[1]['id'] == sdk.get_device_id(device_tag="110")

    measures = sdk.get_all_measures()
    # make sure there are 4 measures
    assert len(measures) == 1
    # make sure each measure was inserted correctly
    measure_id_1 = sdk.get_measure_id(measure_tag="MDC_RESP_RATE", freq=0, freq_units="nHz", units="MDC_DIM_RESP_PER_MIN")
    assert measure_id_1 in measures

    # ensure the values, time and measure units were inserted correctly
    _, read_times, read_values = sdk.get_data(measure_id=measure_id_1, start_time_n=0, end_time_n=1751124948520000000, device_id=devices[1]['id'])

    assert read_values[0] == 28
    assert read_times[0] == 1651124948520000000
    assert read_values[1] == 29
    assert read_times[1] == 1651124949520000000
    assert read_values[2] == 29
    assert read_times[2] == 1651124949520050000
    assert read_values[3] == 30
    assert read_times[3] == 1651124950520000100
    assert read_values[4] == 22
    assert read_times[4] == 1651124955530000000
    assert read_values[5] == 21
    assert read_times[5] == 1651124956590000000
    assert measures[measure_id_1]['unit'] == 'MDC_DIM_RESP_PER_MIN'
    assert measures[measure_id_1]['freq_nhz'] == 0

    print("Aperiodic metric test passed")


def send_data(test_data_dir: str):

    if config.rabbitmq['encrypt']:
        ssl_context = ssl.create_default_context(cafile=config.rabbitmq['certificate_path'])
        ssl_options = pika.SSLOptions(ssl_context, config.rabbitmq['host'])

        credentials = pika.PlainCredentials(config.rabbitmq['username'], config.rabbitmq['password'])
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=config.rabbitmq['host'],
                                      port=config.rabbitmq['port'],
                                      credentials=credentials,
                                      ssl_options=ssl_options,
                                      heartbeat=5))
    else:
        credentials = pika.PlainCredentials(config.rabbitmq['username'], config.rabbitmq['password'])
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(config.rabbitmq['host'],
                                      config.rabbitmq['port'],
                                      '/',
                                      credentials))
    channel = connection.channel()

    # make queue called hello
    channel.queue_declare(queue=config.svc_wal_writer['inbound_queue'], durable=True)

    with open(test_data_dir, 'r') as f:
        for line in f:
            channel.basic_publish(exchange='',
                                  routing_key=config.svc_wal_writer['inbound_queue'],
                                  body=line.strip(),
                                  # this is needed when queue_declare uses dureable=true (makes things persistant in the event of
                                  # an unexpected shutdown)
                                  properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE))
    print("Messages Queued")

    connection.close()


def reset_database(highest_level_dir):
    # drop the database table
    maria_handler = MariaDBHandler(config.CONNECTION_PARAMS['host'], config.CONNECTION_PARAMS['user'], config.CONNECTION_PARAMS['password'], db_name)
    maria_handler.maria_connect_no_db().cursor().execute(f"DROP DATABASE IF EXISTS `{db_name}`")

    db_path = f"{highest_level_dir}/meta/index.db"
    tsc_path = f"{highest_level_dir}/tsc"
    wal_path = f"{highest_level_dir}/wal"

    Path(db_path).unlink(missing_ok=True)

    if Path(tsc_path).is_dir():
        shutil.rmtree(tsc_path)
    os.mkdir(highest_level_dir + "/tsc")

    if Path(wal_path).is_dir():
        shutil.rmtree(wal_path)
    os.mkdir(highest_level_dir + "/wal")


if __name__ == "__main__":
    test_end_to_end_met()
    test_end_to_end_aperiodic_met()
    test_end_to_end_wav()
    test_end_to_end_wav_noscale()
