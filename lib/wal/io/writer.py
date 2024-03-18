from typing import Union
import hashlib
import random

import numpy as np
import struct
import orjson
import os.path

from wal.io.data import value_data_type_dict, value_struct_char_dict, supported_versions, WALData, \
    value_py_type_dict
from wal.io.header_structure import header_attribute_list, get_header_structure_from_dict, \
    WALHeaderStructure


class WALWriter:

    def __init__(self, directory, filename):
        self.directory = os.path.abspath(directory)
        self.value_dtype = None
        self.value_struct_char = None
        self.value_py_type = None
        self.samples_per_message = None
        self.filename = '/'.join((self.directory, filename))
        self.current_file_pointer = open(self.filename, 'wb')

    @classmethod
    def from_metadata(cls, directory, metadata, suffix=None):
        if type(suffix) == int or type(suffix) == str:
            # Do nothing
            pass
        elif suffix is None:
            suffix = int(random.getrandbits(64))
        else:
            suffix = cls._hash_metadata(suffix)

        filename = "{}-{}.wal".format(cls._hash_metadata(metadata), suffix)
        return cls(directory, filename)

    def __del__(self):
        self.close()

    def close(self):
        if not self.current_file_pointer.closed:
            self.current_file_pointer.close()

    def write_header(self, header):
        if type(header) is WALHeaderStructure:
            pass
        elif type(header) is dict:
            header = get_header_structure_from_dict(header)
        else:
            raise TypeError("header must be of type {}.", [WALHeaderStructure, dict])

        assert header.version in supported_versions

        self.value_dtype = value_data_type_dict[header.input_value_type]
        self.value_struct_char = value_struct_char_dict[header.input_value_type]
        self.value_py_type = value_py_type_dict[header.input_value_type]
        self.samples_per_message = header.samples_per_message

        self.current_file_pointer.write(bytearray(header))
        self.current_file_pointer.flush()

    def write_interval_message(self, start_time_nominal: int, start_time_server: int, values: np.ndarray,
                               num_values: int = None, null_offset: int = 0):
        assert values.dtype == self.value_dtype

        num_values = int(values.size) if num_values is None else int(num_values)

        self.current_file_pointer.write(
            struct.pack("<qqII", int(start_time_nominal), int(start_time_server), int(num_values), int(null_offset)))
        self.current_file_pointer.write(values.tobytes())
        self.current_file_pointer.flush()

    def write_time_value_pair_message(self, time_nominal: int, time_server: int, value: Union[int, float]):
        assert self.value_struct_char is not None
        self.current_file_pointer.write(
            struct.pack("<qq" + self.value_struct_char,
                        int(time_nominal), int(time_server), self.value_py_type(value)))
        self.current_file_pointer.flush()

    def write_wal_data(self, wal_data: WALData):
        self.current_file_pointer.write(wal_data.byte_arr.tobytes())
        self.current_file_pointer.flush()

    def flush(self):
        self.current_file_pointer.flush()

    @staticmethod
    def _hash_metadata(metadata):
        if isinstance(metadata, dict):
            # convert any values that are bytes in the dictionary to string so it can serialize
            meta = {k: v.decode('utf-8') if type(v) == bytes else v for k, v in metadata.items()}

        elif isinstance(metadata, WALHeaderStructure):
            meta = {field: getattr(metadata, field).decode('utf-8') if type(getattr(metadata, field)) == bytes else getattr(metadata, field) for field, _ in metadata._fields_}

        meta = orjson.dumps(meta, option=orjson.OPT_SORT_KEYS)
        return hashlib.md5(meta).hexdigest()


def get_null_header_dictionary():
    return dict.fromkeys(header_attribute_list)
