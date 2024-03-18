import ctypes
import struct
import copy

import numpy as np

from wal.io.enums import ValueMode
from wal.io.header_structure import WALHeaderStructure, get_header_structure_from_dict, \
    get_wal_header_struct_size

NANO = 10 ** 9

value_data_type_dict = {0: np.dtype("<f4"), 1: np.dtype("<f8"), 2: np.dtype("<i1"), 3: np.dtype("<i2"),
                        4: np.dtype("<i4"), 5: np.dtype("<i8")}
# python iso standard for number types
value_struct_char_dict = {0: 'f', 1: 'd', 2: 'b', 3: 'h', 4: 'i', 5: 'q'}
value_py_type_dict = {0: float, 1: float, 2: int, 3: int, 4: int, 5: int}
time_data_data_type = np.dtype('<i8')
value_metadata_data_type = np.dtype('<u4')
data_type_byte = np.uint8

header_size = ctypes.sizeof(WALHeaderStructure)
interval_message_struct_types = '<qqII'
interval_message_header_size = struct.calcsize(interval_message_struct_types)

supported_versions = [1]


class WALData:
    def __init__(self, byte_arr=None):
        self.header = None
        self.data = None

        self.time_data = None
        self.server_time_data = None

        self.value_data = None
        self.message_sizes = None
        self.null_offsets = None

        self.byte_arr = byte_arr

    def __eq__(self, other):
        if self.byte_arr is not None and self._bytes_equal(self.byte_arr, other.byte_arr):
            return True
        if not self._bytes_equal(self.header, other.header):
            return False
        if self._numpy_not_equal(self.time_data, other.time_data):
            return False
        if self._numpy_not_equal(self.server_time_data, other.server_time_data):
            return False
        if self._numpy_not_equal(self.value_data, other.value_data):
            return False
        return True

    @classmethod
    def from_file(cls, path):
        return cls(byte_arr=np.fromfile(path, dtype=data_type_byte))

    @classmethod
    def from_interval_data(cls, header_dictionary, nominal_times, server_times, value_messages, messages_sizes=None,
                           null_offsets=None):
        assert header_dictionary['mode'] == ValueMode.INTERVALS.value
        assert len(value_messages.shape) == 2
        result = cls()
        result.header = get_header_structure_from_dict(header_dictionary)

        result.time_data = nominal_times
        result.server_time_data = server_times

        result.value_data = value_messages
        result.message_sizes = messages_sizes if messages_sizes is not None else result._guess_full_messages()
        result.null_offsets = null_offsets if null_offsets is not None else result._guess_no_offsets()

        return result

    @classmethod
    def from_time_value_data(cls, header_dictionary, nominal_times, server_times, values):
        assert header_dictionary['mode'] == ValueMode.TIME_VALUE_PAIRS.value
        result = cls()
        result.header = get_header_structure_from_dict(header_dictionary)

        result.time_data = nominal_times
        result.server_time_data = server_times

        result.value_data = values

        return result

    @staticmethod
    def _numpy_not_equal(arr_1, arr_2):
        if arr_1 is None or arr_2 is None:
            return arr_2 is not arr_1
        return not np.array_equal(arr_1, arr_2)

    @staticmethod
    def _bytes_equal(b_1, b_2):
        if b_1 is None or b_2 is None:
            return b_1 is b_2
        return bytearray(b_1) == bytearray(b_2)

    def interpret_byte_array(self):
        self.header = WALHeaderStructure.from_buffer(self.byte_arr)
        assert self.header.version in supported_versions

        if self.header.mode == ValueMode.TIME_VALUE_PAIRS.value:
            # Time-Value Pair
            self._interpret_time_value_pairs()

        elif self.header.mode == ValueMode.INTERVALS.value:
            # Intervals
            if self.header.samples_per_message == 0:
                self._interpret_intervals_line_by_line()
            else:
                self._interpret_intervals()

        else:
            raise ValueError("{} mode not in {}".format(self.header.mode, list(ValueMode)))

    def prepare_byte_array(self):
        if self.header.mode == ValueMode.INTERVALS.value and self.header.samples_per_message == 0:
            self._prepare_interval_data_line_by_line()
            return

        data_type = self._get_prepared_data_type()

        bytearray_size = get_wal_header_struct_size() + (data_type.itemsize * self.time_data.size)

        self.byte_arr = np.empty(bytearray_size, dtype=data_type_byte)
        self.byte_arr[:get_wal_header_struct_size()] = np.frombuffer(bytearray(self.header), dtype=data_type_byte)

        self.data = self.byte_arr[get_wal_header_struct_size():].view(dtype=data_type)

        self._prepare_data()

    def _prepare_interval_data_line_by_line(self):
        # Determine the total size of the byte array
        header_size = get_wal_header_struct_size()
        value_dtype = value_data_type_dict[self.header.input_value_type]
        total_size = header_size

        # Calculate total size based on individual message sizes
        for num_values in self.message_sizes:
            total_size += interval_message_header_size + num_values * np.dtype(value_dtype).itemsize

        # Initialize the byte array
        self.byte_arr = np.empty(total_size, dtype=np.uint8)
        # Insert header
        self.byte_arr[:header_size] = np.frombuffer(bytearray(self.header), dtype=np.uint8)

        # Fill in the data
        offset = header_size
        value_offset = 0
        concatenated_value_data = np.concatenate(
                        [v[:self.message_sizes[i]] for i, v in enumerate(self.value_data)], axis=None)
        for i in range(len(self.time_data)):
            # Prepare message header
            message_header = struct.pack("<qqII", int(self.time_data[i]), int(self.server_time_data[i]),
                                         int(self.message_sizes[i]), int(self.null_offsets[i]))
            self.byte_arr[offset:offset + interval_message_header_size] = np.frombuffer(message_header, dtype=np.uint8)
            offset += interval_message_header_size

            # Insert message values
            values_size = self.message_sizes[i] * np.dtype(value_dtype).itemsize
            value_slice = concatenated_value_data[value_offset:value_offset + self.message_sizes[i]]
            values_bytes = value_slice.tobytes()
            self.byte_arr[offset:offset + values_size] = np.frombuffer(values_bytes, dtype=np.uint8)
            offset += values_size
            value_offset += self.message_sizes[i]

    def _get_prepared_data_type(self):
        if self.header.mode == ValueMode.TIME_VALUE_PAIRS.value:
            data_type = self._get_time_value_data_type()

        elif self.header.mode == ValueMode.INTERVALS.value:
            # Intervals
            data_type = self._get_interval_data_type()

        else:
            raise ValueError("{} mode not in {}".format(self.header.mode, list(ValueMode)))
        return data_type

    def _prepare_data(self):
        if self.header.mode == ValueMode.TIME_VALUE_PAIRS.value:
            self.data['nominal_time'] = self.time_data
            self.data['server_time'] = self.server_time_data
            self.data['value'] = self.value_data

        elif self.header.mode == ValueMode.INTERVALS.value:
            self.data['start_time_nominal'] = self.time_data
            self.data['start_time_server'] = self.server_time_data
            self.data['values'] = self.value_data

            self.data['num_values'] = self.message_sizes
            self.data['null_offset'] = self.null_offsets

        else:
            raise ValueError("{} mode not in {}".format(self.header.mode, list(ValueMode)))

    def _interpret_time_value_pairs(self):
        data_type = self._get_time_value_data_type()

        body_arr = self.byte_arr[header_size:]
        # Truncate files that ended mid message.
        data_remainder = body_arr.size % data_type.itemsize
        if data_remainder != 0:
            extra_bytes = body_arr[-data_remainder:]
            body_arr = body_arr[:-data_remainder]

        self.data = np.frombuffer(body_arr, dtype=data_type)

        self.time_data = self.data["nominal_time"]
        self.server_time_data = self.data["server_time"]

        self.value_data = self.data["value"]

    def _get_time_value_data_type(self):
        data_type = np.dtype([('nominal_time', time_data_data_type),
                              ('server_time', time_data_data_type),
                              ('value', value_data_type_dict[self.header.input_value_type])])
        return data_type

    def _interpret_intervals(self):
        data_type = self._get_interval_data_type()

        body_arr = self.byte_arr[header_size:]
        # Truncate files that ended mid message.
        data_remainder = body_arr.size % data_type.itemsize
        if data_remainder != 0:
            extra_bytes = body_arr[-data_remainder:]
            body_arr = body_arr[:-data_remainder]

        self.data = np.frombuffer(body_arr, dtype=data_type)

        self.time_data = self.data["start_time_nominal"]
        self.server_time_data = self.data["start_time_server"]

        self.value_data = self.data["values"]
        self.message_sizes = self.data["num_values"]
        self.null_offsets = self.data["null_offset"]

    def _interpret_intervals_line_by_line(self):
        header_size = ctypes.sizeof(WALHeaderStructure)
        body_arr = self.byte_arr[header_size:]

        # Prepare lists to hold parsed data
        time_data = []
        server_time_data = []
        value_data = []
        message_sizes = []
        null_offsets = []

        # Determine value data type and its size
        value_dtype = value_data_type_dict[self.header.input_value_type]
        value_size = np.dtype(value_dtype).itemsize

        # Initialize cursor for tracking position within body_arr
        cursor = 0
        while cursor < len(body_arr):
            if cursor + interval_message_header_size >= len(body_arr):
                break
            # Parse message header
            start_time_nominal, start_time_server, num_values, null_offset = struct.unpack_from(
                interval_message_struct_types, body_arr, offset=cursor)
            cursor += interval_message_header_size

            # Record header data
            time_data.append(start_time_nominal)
            server_time_data.append(start_time_server)
            message_sizes.append(num_values)
            null_offsets.append(null_offset)

            # Parse and record value data
            values_end = cursor + num_values * value_size
            if values_end > len(body_arr):
                message_sizes[-1] = 0
                break
            values = np.frombuffer(body_arr[cursor:values_end], dtype=value_dtype)
            value_data.extend(values)
            cursor = values_end

        # Convert lists to NumPy arrays
        self.time_data = np.array(time_data, dtype=np.int64)
        self.server_time_data = np.array(server_time_data, dtype=np.int64)
        self.value_data = np.array(value_data, dtype=value_dtype)
        self.message_sizes = np.array(message_sizes, dtype=np.uint32)
        self.null_offsets = np.array(null_offsets, dtype=np.uint32)

    def _get_interval_data_type(self):
        data_type = np.dtype([("start_time_nominal", time_data_data_type),
                              ("start_time_server", time_data_data_type),
                              ("num_values", value_metadata_data_type),
                              ("null_offset", value_metadata_data_type),
                              ("values", value_data_type_dict[self.header.input_value_type],
                               self.header.samples_per_message)])
        return data_type

    def _guess_full_messages(self):
        result = np.zeros(self.value_data.shape[0], dtype=value_metadata_data_type)
        result += self.value_data.shape[1]
        return result

    def _guess_no_offsets(self):
        result = np.zeros(self.value_data.shape[0], dtype=value_metadata_data_type)
        return result

    def copy(self):
        # Create a new instance of WALData without initializing its attributes
        new_copy = WALData()

        # Copy each attribute
        new_copy.data = copy.deepcopy(self.data)
        new_copy.time_data = copy.deepcopy(self.time_data)
        new_copy.server_time_data = copy.deepcopy(self.server_time_data)
        new_copy.value_data = copy.deepcopy(self.value_data)
        new_copy.message_sizes = copy.deepcopy(self.message_sizes)
        new_copy.null_offsets = copy.deepcopy(self.null_offsets)

        if self.byte_arr is not None:
            new_copy.byte_arr = copy.deepcopy(self.byte_arr)

        # For the header, which is a ctypes.Structure, create a new instance and copy the fields
        if self.header is not None:
            new_copy.header = WALHeaderStructure()
            ctypes.pointer(new_copy.header)[0] = copy.deepcopy(ctypes.pointer(self.header)[0])

        return new_copy
