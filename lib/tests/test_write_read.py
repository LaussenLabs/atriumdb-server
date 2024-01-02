import os
import unittest
import time
import numpy as np

from wal.io.data import NANO, WALData
from wal.io.enums import ValueMode, ValueType, ScaleType
from wal.io.reader import WALReader
from wal.io.writer import WALWriter
from tests.wal_data_generator import generate_test_data


class TestNData(unittest.TestCase):
    #TODO make test cases automatically generate values rather than hardcoding
    def test_n_messages(self):
        data_directory = "."
        max_messages = 10 ** 3
        mode = ValueMode.INTERVALS.value
        file_start_time = int(time.time()) * NANO
        version = 1
        device_name = bytes("104", 'utf-8')
        sample_freq = 500 * NANO
        samples_per_message = 256
        scale_type = ScaleType.LINEAR.value
        scale_fs = np.array([12.0, 0.0012, 0.0, 0.0], dtype=np.dtype("<f8"))
        input_value_type = ValueType['INT32'].value
        true_value_type = ValueType['FLOAT64'].value
        measure_name = bytes("MDC_RESP", 'utf-8')
        measure_units = bytes("MDC_DIM_X_OHM", 'utf-8')

        header_dict, times, server_times, values = \
            generate_test_data(device_name, input_value_type, max_messages, mode, sample_freq, samples_per_message,
                               scale_fs, scale_type, file_start_time, true_value_type, version, measure_name,
                               measure_units)

        for num_messages in range(max_messages):
            self._test_interval_example(data_directory, header_dict, num_messages, samples_per_message, server_times,
                                        times, values)

    def _test_interval_example(self, data_directory, header_dict, num_messages, samples_per_message, server_times,
                               times, values):
        t = times[:num_messages * samples_per_message:samples_per_message]
        s_t = server_times[:num_messages * samples_per_message:samples_per_message]
        v = values[:num_messages * samples_per_message]
        # Create Writer Object
        writer = WALWriter.from_metadata(data_directory, header_dict)
        filename = writer.filename
        # Write
        writer.write_header(header_dict)

        for message_i in range(num_messages):
            message_vs = v[message_i * samples_per_message:(message_i + 1) * samples_per_message]
            writer.write_interval_message(int(t[message_i]), int(s_t[message_i]), message_vs)
        # Close the Writer
        del writer
        # Read
        reader = WALReader(filename)
        data = reader.read_all()
        os.remove(filename)
        data.interpret_byte_array()

        data_2 = WALData.from_interval_data(header_dict, t, s_t, v.reshape((num_messages, samples_per_message)))

        self.assertTrue(data == data_2)
        self.assertTrue(np.array_equal(t, data.time_data), "times num messages {} - Arr: {} - Dtype: {}".format(
            num_messages, data.time_data, data.time_data.dtype))
        self.assertTrue(np.array_equal(s_t, data.server_time_data),
                        "server times num messages {} - Arr: {} - Dtype: {}".format(
                            num_messages, data.time_data, data.time_data.dtype))
        self.assertTrue(np.array_equal(v.reshape((-1, samples_per_message)), data.value_data),
                        "values num messages {} data - Arr: {} - Dtype: {} - Original - Arr: {} - Dtype: {}".format(
                            num_messages, data.value_data, data.value_data.dtype, values, values.dtype))

    def test_n_pairs(self):
        data_directory = "."
        max_messages = 10 ** 3
        mode = ValueMode.TIME_VALUE_PAIRS.value
        file_start_time = int(time.time()) * NANO
        version = 1
        device_name = bytes("104", 'utf-8')
        sample_freq = 500 * NANO
        samples_per_message = 1
        scale_type = ScaleType.LINEAR.value
        scale_fs = np.array([12.0, 0.0012, 0.0, 0.0], dtype=np.dtype("<f8"))
        input_value_type = ValueType['INT32'].value
        true_value_type = ValueType['FLOAT64'].value
        measure_name = bytes("MDC_RESP", 'utf-8')
        measure_units = bytes("MDC_DIM_X_OHM", 'utf-8')

        header_dict, times, server_times, values = \
            generate_test_data(device_name, input_value_type, max_messages, mode, sample_freq, samples_per_message,
                               scale_fs, scale_type, file_start_time, true_value_type, version, measure_name,
                               measure_units)

        # Run Tests
        for num_messages in range(max_messages + 1):
            self._test_pair_example(data_directory, header_dict, num_messages, server_times, times, values)

    def _test_pair_example(self, data_directory, header_dict, num_messages, server_times, times, values):
        # Select N messages
        t = times[:header_dict['samples_per_message'] * num_messages]
        s_t = server_times[:header_dict['samples_per_message'] * num_messages]
        v = values[:header_dict['samples_per_message'] * num_messages]
        # Create Writer Object
        writer = WALWriter.from_metadata(data_directory, header_dict)
        filename = writer.filename
        # Write
        writer.write_header(header_dict)

        for message_i in range(num_messages):
            writer.write_time_value_pair_message(t[message_i], s_t[message_i], v[message_i])
        # Close the Writer
        del writer
        # Read
        reader = WALReader(filename)
        data = reader.read_all()
        os.remove(filename)
        data.interpret_byte_array()

        data_2 = WALData.from_time_value_data(header_dict, t, s_t, v)
        self.assertTrue(data == data_2)
        self.assertTrue(np.array_equal(t, data.time_data), "times num messages {} - Arr: {} - Dtype: {}".format(
            num_messages, data.time_data, data.time_data.dtype))
        self.assertTrue(np.array_equal(s_t, data.server_time_data),
                        "server times num messages {} - Arr: {} - Dtype: {}".format(
                            num_messages, data.time_data, data.time_data.dtype))
        self.assertTrue(np.array_equal(v, data.value_data), "values num messages {} - Arr: {} - Dtype: {}".format(
            num_messages, data.value_data, data.value_data.dtype))


if __name__ == '__main__':
    unittest.main()
