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

    # get the measure_id from the measure tag
    measure_id = sdk.get_measure_id(measure_tag=h.measure_name.decode('utf-8'), freq=h.sample_freq, units=h.measure_units.decode('utf-8'))
    if measure_id is None:
        # insert the measure if it does not exist
        sdk.insert_measure(measure_tag=h.measure_name.decode('utf-8'), freq=h.sample_freq, units=h.measure_units.decode('utf-8'))
        measure_id = sdk.get_measure_id(measure_tag=h.measure_name.decode('utf-8'), freq=h.sample_freq, units=h.measure_units.decode('utf-8'))
        if measure_id is None:
            _LOGGER.error("Failed to insert measure into AtriumDB. Measure_tag={}, frequency={}, units={}".format(h.measure_name.decode('utf-8'), h.sample_freq, h.measure_units.decode('utf-8')))
            return -2
        else:
            measures_inserted_counter.add(1)

    # get the device_id from the tag
    device_id = sdk.get_device_id(device_tag=h.device_name.decode('utf-8'))
    if device_id is None:
        # insert a new device if the device does not exist
        sdk.insert_device(device_tag=h.device_name.decode('utf-8'))
        device_id = sdk.get_device_id(device_tag=h.device_name.decode('utf-8'))
        if device_id is None:
            _LOGGER.error("Failed to insert device into AtriumDB. Device_tag={}".format(h.device_name.decode('utf-8')))
            return -2
        else:
            devices_inserted_counter.add(1)

    time_arr = []
    # this mode is generally used for metrics
    if h.mode == ValueMode.TIME_VALUE_PAIRS.value:
        value_data = wal_data.value_data

        if h.sample_freq != 0:
            time_arr = create_gap_arr(wal_data.time_data, h.samples_per_message, h.sample_freq)
        else:
            # if the frequency is 0 we know the signal is aperiodic so don't try to make a gap array
            time_arr = wal_data.time_data

    # this mode is generally used for waveforms
    elif h.mode == ValueMode.INTERVALS.value:
        if h.samples_per_message == 0:
            value_data = wal_data.value_data
            time_arr = create_gap_arr_from_variable_messages(wal_data.time_data, wal_data.message_sizes, h.sample_freq)
        else:
            # legacy code shouldn't be used. Kept for reverse compatibility
            value_data = np.concatenate([v[:wal_data.message_sizes[i]] for i, v in enumerate(wal_data.value_data)], axis=None)
            time_arr = create_gap_arr(wal_data.time_data, h.samples_per_message, h.sample_freq)
    else:
        raise ValueError("{} not in {}.".format(h.mode, list(ValueMode)))

    if np.issubdtype(value_data.dtype, np.integer):
        raw_v_t = V_TYPE_INT64
        encoded_v_t = V_TYPE_DELTA_INT64
    else:
        raw_v_t = V_TYPE_DOUBLE
        encoded_v_t = V_TYPE_DOUBLE

    if h.sample_freq != 0:
        t_t = T_TYPE_GAP_ARRAY_INT64_INDEX_DURATION_NANO
        gap_tolerance = config.svc_tsc_gen['gap_tolerance']

    # if the freq=0 and we are in time value pair mode that means the signal is aperiodic
    elif h.mode == ValueMode.TIME_VALUE_PAIRS.value:
        # set time type to timestamp array
        t_t = T_TYPE_TIMESTAMP_ARRAY_INT64_NANO
        # turn on time compression for the times since they will all be saved
        sdk.block.t_compression = 3
        sdk.block.t_compression_level = 13

        # set the gap tolerance to 2 times the biggest gap in the wal file and if there's only one value make it 1 hour
        gap_tolerance = int(np.max(np.diff(time_arr[1:] - time_arr[:-1]) * 2)) if len(time_arr) > 1 else 3_600_000_000_000
        # set frequency to 1 so things in the sdk don't break when calculating the intervals
        h.sample_freq = 1
    else:
        raise NotImplementedError("Aperiodic mode (freq=0) has not been implemented for gap arrays yet")


    sdk.write_data(
        measure_id, device_id, time_arr, value_data, h.sample_freq, int(wal_data.time_data[0]),
        raw_time_type=t_t, raw_value_type=raw_v_t, encoded_time_type=t_t, encoded_value_type=encoded_v_t,
        scale_b=h.scale_0, scale_m=h.scale_1, interval_index_mode=config.svc_tsc_gen['interval_index_mode'],
        gap_tolerance=gap_tolerance)

    # reset time compression back to normal values incase they were changed by an aperiodic signal
    sdk.block.t_compression = 1
    sdk.block.t_compression_level = 0

    return 0


def trim_corrupt_data(wal_data):
    if wal_data.header.mode == ValueMode.TIME_VALUE_PAIRS.value:
        return 0
    elif wal_data.header.mode == ValueMode.INTERVALS.value:
        if wal_data.header.samples_per_message == 0:
            return 0
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


def create_gap_arr_from_variable_messages(time_data, message_sizes, sample_freq):
    sample_freq = int(sample_freq)
    result_list = []
    current_sample = 0

    for i in range(1, len(time_data)):
        # Compute the time difference between consecutive messages
        delta_t = time_data[i] - time_data[i - 1]

        # Calculate the message period for the current message based on its size
        current_message_size = int(message_sizes[i - 1])
        current_message_period_ns = ((10 ** 18) * current_message_size) // sample_freq

        # Check if the time difference doesn't match the expected message period
        if delta_t != current_message_period_ns:
            # Compute the extra duration (time gap) and the starting index of the gap
            time_gap = delta_t - current_message_period_ns
            gap_start_index = current_sample + current_message_size

            # Add the gap information to the result list
            result_list.extend([gap_start_index, time_gap])

        # Update the current sample index for the next iteration
        current_sample += current_message_size

    # Convert the result list to a NumPy array of integers
    return np.array(result_list, dtype=np.int64)
