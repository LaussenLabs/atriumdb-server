import os
import random
import unittest
import copy
import numpy as np
import string

from wal.io.data import WALData
from wal.io.enums import ValueMode
from wal.io.reader import WALReader
from wal.io.writer import WALWriter
from tests.wal_data_generator import generate_random_header_dict, generate_time_data_from_header, \
    generate_value_data_from_header, generate_wal_data_arr


class TestWALData(unittest.TestCase):

    def test_pure_equality(self):
        # Set the initial variables
        wal_matrix_size = 10

        num_messages = 10 ** 3
        for enum_mode in list(ValueMode):
            mode = enum_mode.value

            # Generate the Data
            wal_data_matrix = self._generate_wal_data_matrix(mode, num_messages, wal_matrix_size)

            # Test all elements against all other elements
            for wal_data_1 in wal_data_matrix.ravel():
                for wal_data_2 in wal_data_matrix.ravel():
                    if wal_data_1 is wal_data_2:
                        self.assertTrue(wal_data_1 == wal_data_2)

                        wal_data_1_copy = copy.deepcopy(wal_data_1)
                        self.assertTrue(wal_data_1_copy == wal_data_2)
                        wal_data_1_copy.time_data[0] += 1

                        self.assertTrue(wal_data_1_copy != wal_data_2)

                    else:
                        self.assertTrue(wal_data_1 != wal_data_2)

    def test_read_write_equality(self):
        # Set the initial variables
        wal_arr_size = 100

        num_messages = 10 ** 3
        for enum_mode in list(ValueMode):
            mode = enum_mode.value

            # Generate the Data
            wal_data_arr = generate_wal_data_arr(mode, num_messages, wal_arr_size)

            wal_data_full_write_read_arr = np.empty(wal_data_arr.size, dtype=WALData)
            wal_data_incremental_write_read_arr = np.empty(wal_data_arr.size, dtype=WALData)

            for wal_i, wal_data in np.ndenumerate(wal_data_arr):
                # Total write -> read
                writer = WALWriter.from_metadata(
                    ".", wal_data.header,
                    suffix=random.choice([random.randint(0, 10 ** 9),
                                          ''.join([random.choice(string.ascii_letters)
                                                   for _ in range(random.randint(1, 16))]),
                                          wal_data.header,
                                          None]))

                wal_data.prepare_byte_array()
                writer.write_wal_data(wal_data)
                writer.close()

                reader = WALReader(writer.filename)
                full_read_wal_data = reader.read_all()
                os.remove(writer.filename)
                full_read_wal_data.interpret_byte_array()

                self.assertTrue(full_read_wal_data == wal_data)

                wal_data_full_write_read_arr[wal_i] = full_read_wal_data

                # Incremental write -> read
                writer = WALWriter.from_metadata(".", wal_data.header)
                writer.write_header(wal_data.header)

                if mode == ValueMode.TIME_VALUE_PAIRS.value:
                    for message_i in range(wal_data.time_data.size):
                        writer.write_time_value_pair_message(
                            wal_data.time_data[message_i],
                            wal_data.server_time_data[message_i],
                            wal_data.value_data[message_i])

                elif mode == ValueMode.INTERVALS.value:
                    for message_i in range(wal_data.time_data.size):
                        writer.write_interval_message(
                            wal_data.time_data[message_i],
                            wal_data.server_time_data[message_i],
                            wal_data.value_data[message_i])

                writer.close()
                reader = WALReader(writer.filename)
                incremental_read_wal_data = reader.read_all()
                os.remove(writer.filename)
                incremental_read_wal_data.interpret_byte_array()

                self.assertTrue(incremental_read_wal_data == wal_data)

                wal_data_incremental_write_read_arr[wal_i] = incremental_read_wal_data

    @staticmethod
    def _generate_wal_data_matrix(mode, num_messages, wal_matrix_size):
        headers = [generate_random_header_dict(mode=mode) for _ in range(wal_matrix_size)]
        time_list = [generate_time_data_from_header(headers[i], num_messages) for i in range(wal_matrix_size)]
        value_list = [generate_value_data_from_header(headers[i], num_messages) for i in range(wal_matrix_size)]
        if mode == ValueMode.TIME_VALUE_PAIRS.value:
            wal_data_matrix = [[[WALData.from_time_value_data(h, t[0], t[1], v)
                                 for h in headers]
                                for t in time_list]
                               for v in value_list]

        elif mode == ValueMode.INTERVALS.value:
            wal_data_matrix = [[[WALData.from_interval_data(h, t[0], t[1], v[0], v[1], v[2])
                                 for h in headers]
                                for t in time_list]
                               for v in value_list]

        else:
            raise ValueError("{} not in {}.".format(mode, list(ValueMode)))
        wal_data_matrix = np.array(wal_data_matrix, dtype=WALData)
        return wal_data_matrix


if __name__ == '__main__':
    unittest.main()
