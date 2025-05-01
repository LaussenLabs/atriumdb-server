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
import os
import random
import unittest
import copy
import numpy as np
import string

from atriumdb import create_gap_arr

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

                        # wal_data_1_copy = copy.deepcopy(wal_data_1)
                        wal_data_1_copy = wal_data_1.copy()
                        self.assertTrue(wal_data_1_copy == wal_data_2)
                        wal_data_1_copy.time_data[0] += 1

                        self.assertTrue(wal_data_1_copy != wal_data_2)

                    else:
                        self.assertTrue(wal_data_1 != wal_data_2)

    def test_read_write_equality(self):
        # Set the initial variables
        wal_arr_size = 100
        # wal_arr_size = 1

        num_messages = 10 ** 3
        # num_messages = 10
        for enum_mode in list(ValueMode):
            mode = enum_mode.value

            # Generate the Data
            wal_data_arr = generate_wal_data_arr(mode, num_messages, wal_arr_size)

            wal_data_full_write_read_arr = np.empty(wal_data_arr.size, dtype=WALData)
            wal_data_incremental_write_read_arr = np.empty(wal_data_arr.size, dtype=WALData)

            for wal_i, wal_data in np.ndenumerate(wal_data_arr):
                # Create a copy in readline mode
                wal_data_readline_copy = wal_data.copy()
                wal_data_readline_copy.header.samples_per_message = 0

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

                # Also write the copy in readline mode.
                writer_readline = WALWriter.from_metadata(".", wal_data_readline_copy.header)
                wal_data_readline_copy.prepare_byte_array()
                writer_readline.write_wal_data(wal_data_readline_copy)
                writer_readline.close()

                # read and delete both files
                reader = WALReader(writer.filename)
                full_read_wal_data = reader.read_all()
                os.remove(writer.filename)
                full_read_wal_data.interpret_byte_array()

                reader_readline = WALReader(writer_readline.filename)
                full_read_wal_data_readline = reader_readline.read_all()
                os.remove(writer_readline.filename)
                full_read_wal_data_readline.interpret_byte_array()

                self.assertTrue(full_read_wal_data == wal_data)

                # Test readline mode
                if wal_data.header.mode == ValueMode.TIME_VALUE_PAIRS.value:
                    self.assertTrue(np.array_equal(
                        full_read_wal_data_readline.value_data, wal_data.value_data))

                elif wal_data.header.mode == ValueMode.INTERVALS.value:
                    value_data = np.concatenate(
                        [v[:wal_data.message_sizes[i]] for i, v in enumerate(wal_data.value_data)], axis=None)
                    self.assertTrue(np.array_equal(
                        full_read_wal_data_readline.value_data, value_data))

                    # Test gap array equality
                    normal_gap_array = create_gap_arr(
                        wal_data.time_data, wal_data.header.samples_per_message, wal_data.header.sample_freq)

                    readline_gap_arr = create_gap_arr_from_variable_messages(
                        full_read_wal_data_readline.time_data,
                        full_read_wal_data_readline.message_sizes,
                        full_read_wal_data_readline.header.sample_freq)

                    self.assertTrue(len(readline_gap_arr) == len(normal_gap_array))
                    self.assertTrue(np.array_equal(readline_gap_arr, normal_gap_array))

                else:
                    raise ValueError(f"wal data mode {wal_data.header.mode} must be one of "
                                     f"{[ValueMode.TIME_VALUE_PAIRS.value, ValueMode.INTERVALS.value]}")

                self.assertTrue(np.array_equal(
                    full_read_wal_data_readline.time_data, wal_data.time_data))

                self.assertTrue(np.array_equal(
                    full_read_wal_data_readline.message_sizes, wal_data.message_sizes))

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


if __name__ == '__main__':
    unittest.main()
