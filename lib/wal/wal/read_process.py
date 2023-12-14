import os
from concurrent.futures import ThreadPoolExecutor

import numpy as np

from wal.batch import WALBatch
from wal.io.data import WALData
from wal.io.enums import ValueMode
from wal.io.header_structure import get_wal_header_struct_size
from wal.io.reader import WALReader


def read_batch(batch: WALBatch, ingest_function, *args, delete_on_ingest=True):
    wal_data_list = [data for data in io_read_batch(batch.get_paths()) if data is not None]

    if len(wal_data_list) > 0:
        merged_wal_data = merge_data(wal_data_list)
        batch.result = ingest_function(merged_wal_data, *args)
    else:
        batch.result = None

    if delete_on_ingest and batch.result != -1:
        batch.delete_all_paths()

    return batch


def io_read_batch(paths):
    with ThreadPoolExecutor(max_workers=len(paths)) as executor:
        return list(executor.map(io_read_file, paths))


def io_read_file(path):
    reader = WALReader(path)
    data = reader.read_all()
    if len(data.byte_arr) < get_wal_header_struct_size():
        return None
    data.interpret_byte_array()
    return data


def merge_data(wal_data_list):
    # Only use the first header and ignore the rest.
    header = wal_data_list[0].header
    result = wal_data_list[0]

    # Concatenate All Data
    if len(wal_data_list) > 1:
        result.time_data = np.concatenate([wd.time_data for wd in wal_data_list], axis=None)
        result.server_time_data = np.concatenate([wd.server_time_data for wd in wal_data_list], axis=None)

        if header.mode == ValueMode.TIME_VALUE_PAIRS.value:
            result.value_data = np.concatenate([wd.value_data for wd in wal_data_list], axis=None)

            result.message_sizes, result.null_offsets = None, None

        elif header.mode == ValueMode.INTERVALS.value:
            result.value_data = np.concatenate([wd.value_data for wd in wal_data_list], axis=None).reshape(
                (-1, header.samples_per_message))

            result.message_sizes = np.concatenate([wd.message_sizes for wd in wal_data_list], axis=None)
            result.null_offsets = np.concatenate([wd.null_offsets for wd in wal_data_list], axis=None)

        else:
            raise ValueError("{} not in {}.".format(header.mode, list(ValueMode)))

    # Sort all data based on nominal time (time_data)
    _, sorted_indices = np.unique(result.time_data, return_index=True, axis=None)

    result.time_data = result.time_data[sorted_indices]
    result.server_time_data = result.server_time_data[sorted_indices]
    result.value_data = result.value_data[sorted_indices]
    if header.mode == ValueMode.TIME_VALUE_PAIRS.value:
        pass

    elif header.mode == ValueMode.INTERVALS.value:
        result.message_sizes = result.message_sizes[sorted_indices]
        result.null_offsets = result.null_offsets[sorted_indices]

    else:
        raise ValueError("{} not in {}.".format(header.mode, list(ValueMode)))

    return result


def delete_files(paths):
    for path in paths:
        os.remove(path)
