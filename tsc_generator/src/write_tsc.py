import numpy as np
import logging
from atriumdb import create_gap_arr
from wal import ValueMode
from config import config
from helpers.metrics import (get_metric,
                             TSCGENERATOR_DEVICES_INSERTED,
                             TSCGENERATOR_MEASURES_INSERTED)

TSC_VERSION_NUM = 2
TSC_VERSION_EXT = 3
TSC_NUM_CHANNELS = 1

# Time Types
T_TYPE_TIMESTAMP_ARRAY_INT64_NANO = 1
T_TYPE_GAP_ARRAY_INT64_INDEX_DURATION_NANO = 2
T_TYPE_GAP_ARRAY_INT64_INDEX_NUM_SAMPLES = 3
T_TYPE_START_TIME_NUM_SAMPLES = 4

# Value Types
V_TYPE_INT64 = 1
V_TYPE_DOUBLE = 2

V_TYPE_DELTA_INT64 = 3
V_TYPE_XOR_DOUBLE = 4

_LOGGER = logging.getLogger(__name__)


def write_wal_data_to_sdk(wal_data, sdk):
    devices_inserted_counter = get_metric(TSCGENERATOR_DEVICES_INSERTED)
    measures_inserted_counter = get_metric(TSCGENERATOR_MEASURES_INSERTED)

    # Check for corrupted messages and trim before ingesting.
    error_code = trim_corrupt_data(wal_data)
    if error_code == -1:
        _LOGGER.error(f"wal_data.header.mode, {wal_data.header.mode} not one of allowed values: "
                      f"{[member.value for member in ValueMode]}")
        return -1

    h = wal_data.header

    measure_id = sdk.get_measure_id(measure_tag=h.measure_name.decode('utf-8'), freq=h.sample_freq, units=h.measure_units.decode('utf-8'))
    if measure_id is None:
        sdk.insert_measure(measure_tag=h.measure_name.decode('utf-8'), freq=h.sample_freq, units=h.measure_units.decode('utf-8'))
        measure_id = sdk.get_measure_id(measure_tag=h.measure_name.decode('utf-8'), freq=h.sample_freq, units=h.measure_units.decode('utf-8'))
        if measure_id is None:
            _LOGGER.error("Failed to insert measure into AtriumDB. Measure_tag={}, frequency={}, units={}".format(h.measure_name.decode('utf-8'), h.sample_freq, h.measure_units.decode('utf-8')))
            return -2
        else:
            measures_inserted_counter.add(1)

    device_id = sdk.get_device_id(device_tag=h.device_name.decode('utf-8'))
    if device_id is None:
        sdk.insert_device(device_tag=h.device_name.decode('utf-8'))
        device_id = sdk.get_device_id(device_tag=h.device_name.decode('utf-8'))
        if device_id is None:
            _LOGGER.error("Failed to insert device into AtriumDB. Device_tag={}".format(h.device_name.decode('utf-8')))
            return -2
        else:
            devices_inserted_counter.add(1)

    # if sdk.measure_device_start_time_exists(measure_id, device_id, int(wal_data.time_data[0])):
    #     _LOGGER.warning("Duplicate data detected for measure_id {},  device_id {} and start time {}".format(measure_id, device_id, int(wal_data.time_data[0])))
    #     return 1

    gap_arr = create_gap_arr(wal_data.time_data, h.samples_per_message, h.sample_freq)

    if h.mode == ValueMode.TIME_VALUE_PAIRS.value:
        value_data = wal_data.value_data
    elif h.mode == ValueMode.INTERVALS.value:
        value_data = np.concatenate([v[:wal_data.message_sizes[i]] for i, v in enumerate(wal_data.value_data)], axis=None)
    else:
        raise ValueError("{} not in {}.".format(h.mode, list(ValueMode)))

    if np.issubdtype(value_data.dtype, np.integer):
        raw_v_t = V_TYPE_INT64
        encoded_v_t = V_TYPE_DELTA_INT64
    else:
        raw_v_t = V_TYPE_DOUBLE
        encoded_v_t = V_TYPE_DOUBLE

    t_t = T_TYPE_GAP_ARRAY_INT64_INDEX_DURATION_NANO

    sdk.write_data(
        measure_id, device_id, gap_arr, value_data, h.sample_freq, int(wal_data.time_data[0]),
        raw_time_type=t_t, raw_value_type=raw_v_t, encoded_time_type=t_t, encoded_value_type=encoded_v_t,
        scale_b=h.scale_0, scale_m=h.scale_1, interval_index_mode=config.svc_tsc_gen['interval_index_mode'])

    return 0


def trim_corrupt_data(wal_data):
    if wal_data.header.mode == ValueMode.TIME_VALUE_PAIRS.value:
        return 0
    elif wal_data.header.mode == ValueMode.INTERVALS.value:
        for i in range(wal_data.message_sizes.size):
            message_size = wal_data.message_sizes[i]
            null_offset = wal_data.null_offsets[i]

            if message_size > wal_data.header.samples_per_message or \
                    null_offset > wal_data.header.samples_per_message:
                _LOGGER.warning(f"Detecting corrupt interval wal data at message {i}\n "
                                f"ingesting data before corruption.")
                truncate_interval_wal_data_upto_message(wal_data, i)
                return 0
        return 0

    else:
        return -1


def truncate_interval_wal_data_upto_message(wal_data, first_corrupt_message_index):
    wal_data.time_data = wal_data.time_data[:first_corrupt_message_index]
    wal_data.server_time_data = wal_data.server_time_data[:first_corrupt_message_index]

    wal_data.value_data = wal_data.value_data[:first_corrupt_message_index]
    wal_data.message_sizes = wal_data.message_sizes[:first_corrupt_message_index]
    wal_data.null_offsets = wal_data.null_offsets[:first_corrupt_message_index]
