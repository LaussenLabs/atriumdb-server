import numpy as np
import time
import random
from pathlib import Path
import string
import ctypes

from wal.io.data import NANO, time_data_data_type, value_data_type_dict, WALData, supported_versions, \
    value_metadata_data_type
from wal.io.enums import ValueMode, ScaleType, ValueType
from wal.io.writer import get_null_header_dictionary, WALWriter


def write_wal_data(directory, mode, num_messages, num_batches, files_per_batch=1):
    Path(directory).mkdir(parents=True, exist_ok=True)
    wal_data_arr = generate_wal_data_arr(mode, num_messages, num_batches)
    batch_filenames = []

    for wal_data in wal_data_arr:
        # Open files_per_batch number of writers and write the header to each.
        wal_writers = [WALWriter.from_metadata(directory, wal_data.header) for _ in range(files_per_batch)]
        [ww.write_header(wal_data.header) for ww in wal_writers]

        # Remember all the filenames
        wal_filenames = [ww.filename for ww in wal_writers]
        batch_filenames.append(wal_filenames)

        # Randomly Write to the files
        if mode == ValueMode.TIME_VALUE_PAIRS.value:
            for i in range(wal_data.time_data.size):
                writer_i = random.randint(0, files_per_batch - 1)
                wal_writers[writer_i].write_time_value_pair_message(
                    wal_data.time_data[i],
                    wal_data.server_time_data[i],
                    wal_data.value_data[i])

        elif mode == ValueMode.INTERVALS.value:
            for i in range(wal_data.time_data.size):
                writer_i = random.randint(0, files_per_batch - 1)
                wal_writers[writer_i].write_interval_message(
                    wal_data.time_data[i],
                    wal_data.server_time_data[i],
                    wal_data.value_data[i])

        else:
            raise ValueError("{} not in {}.".format(mode, list(ValueMode)))

        # Close all Writers
        [ww.close() for ww in wal_writers]
    return batch_filenames, wal_data_arr


def generate_wal_data_arr(mode, num_messages, wal_arr_size):
    headers = [generate_random_header_dict(mode=mode) for _ in range(wal_arr_size)]
    time_list = [generate_time_data_from_header(headers[i], num_messages) for i in range(wal_arr_size)]
    value_list = [generate_value_data_from_header(headers[i], num_messages) for i in range(wal_arr_size)]

    wal_data_arr = np.empty(wal_arr_size, dtype=WALData)

    if mode == ValueMode.TIME_VALUE_PAIRS.value:
        for i, (h, t, v) in enumerate(zip(headers, time_list, value_list)):
            wal_data_arr[i] = WALData.from_time_value_data(h, t[0], t[1], v)
            wal_data_arr[i].prepare_byte_array()

    elif mode == ValueMode.INTERVALS.value:
        for i, (h, t, v) in enumerate(zip(headers, time_list, value_list)):
            wal_data_arr[i] = WALData.from_interval_data(h, t[0], t[1], v[0], v[1], v[2])
            wal_data_arr[i].prepare_byte_array()

    else:
        raise ValueError("{} not in {}.".format(mode, list(ValueMode)))

    return wal_data_arr


def generate_random_header_dict(mode=None):
    version = random.choice(supported_versions)
    characters = string.ascii_letters + string.digits + string.punctuation
    device_name_length = random.randint(0, 64)
    device_name = bytes("".join([random.choice(characters) if i < device_name_length else "\0" for i in range(64)]), 'utf-8')
    mode = random.choice(list(ValueMode)).value if mode is None else mode
    assert ValueMode.has_value(mode)
    samples_per_message = 1 if mode == ValueMode.TIME_VALUE_PAIRS.value else random.randint(2, 1000)
    sample_freq = get_random_sample_freq(samples_per_message, NANO, 1000 * NANO, 10 ** 6)
    file_start_time = int(time.time()) * NANO
    scale_type = ScaleType.LINEAR.value
    scale_fs = np.array([10 * (random.random() - 0.5), random.random(), 0.0, 0.0], dtype=np.dtype("<f8"))
    input_value_type = random.randint(2, 5)
    true_value_type = ValueType['FLOAT64'].value


    measure_length = random.randint(0, 64)
    measure_name = bytes("".join([random.choice(characters) if i < measure_length else "\0" for i in range(64)]), 'utf-8')
    units_length = random.randint(0, 64)
    measure_units = bytes("".join([random.choice(characters) if i < units_length else "\0" for i in range(64)]), 'utf-8')

    # same as above but using string buffer
    # measure_name = ctypes.create_string_buffer(bytes("".join([random.choice(characters) for _ in range(random.randint(0, 64))]), 'utf-8'), size=64).raw
    # measure_units = ctypes.create_string_buffer(bytes("".join([random.choice(characters) for _ in range(random.randint(0, 64))]), 'utf-8'), size=64).raw
    #print(measure_units, measure_units.raw)

    return generate_header_dict(device_name, input_value_type, mode, sample_freq, samples_per_message,
                                scale_fs, scale_type, file_start_time, true_value_type, version, measure_name, measure_units)


def generate_time_data_from_header(header_dict, num_messages):
    message_period = (header_dict["samples_per_message"] * (10 ** 18)) // header_dict["sample_freq"]
    real_time_scale = 0.9 + (0.1 * random.random())
    real_period = float(message_period) * real_time_scale

    nominal_times = np.arange(header_dict['file_start_time'],
                              header_dict['file_start_time'] + (message_period * num_messages),
                              message_period)

    server_times = np.linspace(header_dict['file_start_time'],
                               header_dict['file_start_time'] + (real_period * num_messages),
                               num=num_messages,
                               endpoint=False)

    num_gaps = num_messages // 107
    messages_between_gaps = 106

    for i in range(1, num_gaps):
        gap_i = i * messages_between_gaps
        gap_length = random.randint(1, 10000) * (10 ** 6)
        nominal_times[gap_i:] += gap_length
        server_times[gap_i:] += (float(gap_length) * real_time_scale)

    return nominal_times, server_times.astype(dtype=np.int64)


def generate_value_data_from_header(header_dict, num_messages):
    data_type = value_data_type_dict[header_dict['input_value_type']]
    message_length = header_dict['samples_per_message']

    random_buffer = np.random.default_rng().bytes(num_messages * message_length * data_type.itemsize)
    values = np.frombuffer(random_buffer, dtype=data_type)

    if header_dict["mode"] == ValueMode.TIME_VALUE_PAIRS.value:
        return values
    elif header_dict["mode"] == ValueMode.INTERVALS.value:
        null_offsets = np.zeros(num_messages, dtype=value_metadata_data_type)
        num_samples_arr = np.zeros(num_messages, dtype=value_metadata_data_type) + message_length
        return values.reshape((num_messages, message_length)), num_samples_arr, null_offsets
    else:
        raise ValueError("mode {} not in {}.".format(header_dict["mode"], list(ValueMode)))


def get_random_sample_freq(samples_per_message, start_freq, end_freq, step_freq):
    return random.choice(get_all_allowable_freq(samples_per_message, start_freq, end_freq, step_freq))


def get_all_allowable_freq(samples_per_message, start_freq, end_freq, step_freq):
    return [freq for freq in range(start_freq, end_freq, step_freq) if (samples_per_message * (10 ** 18)) % freq == 0]


def generate_test_data(device_name, input_value_type, max_messages, mode, sample_freq,
                       samples_per_message, scale_fs, scale_type, file_start_time, true_value_type, version,
                       measure_name, measure_units):
    header_dict = generate_header_dict(device_name, input_value_type, mode, sample_freq,
                                       samples_per_message, scale_fs, scale_type, file_start_time, true_value_type,
                                       version, measure_name, measure_units)

    num_values = max_messages * samples_per_message
    period_ns = int((10 ** 18) / sample_freq)
    times = np.arange(
        file_start_time,
        file_start_time + (period_ns * num_values),
        period_ns
    ).astype(
        time_data_data_type)

    server_jitter = np.random.randint(-1000, 1000, times.size).astype(time_data_data_type)
    server_times = times + server_jitter
    values = (np.sin(times) * (10 ** 4)).astype(value_data_type_dict[input_value_type])
    return header_dict, times, server_times, values


def generate_header_dict(device_name, input_value_type, mode, sample_freq, samples_per_message,
                         scale_fs, scale_type, file_start_time, true_value_type, version,
                         measure_name, measure_units):
    header_dict = get_null_header_dictionary()
    header_dict['version'] = version
    header_dict['device_name'] = device_name
    header_dict['mode'] = mode
    header_dict['sample_freq'] = sample_freq
    header_dict['samples_per_message'] = samples_per_message
    header_dict['file_start_time'] = file_start_time
    header_dict['input_value_type'] = input_value_type
    header_dict['true_value_type'] = true_value_type
    header_dict['scale_type'] = scale_type
    header_dict['scale_0'] = float(scale_fs[0])
    header_dict['scale_1'] = float(scale_fs[1])
    header_dict['scale_2'] = float(scale_fs[2])
    header_dict['scale_3'] = float(scale_fs[3])
    header_dict["measure_name"] = measure_name
    header_dict["measure_units"] = measure_units

    return header_dict


if __name__ == "__main__":
    num_messages = 10 ** 6
    for _ in range(10):
        header_dictionary = generate_random_header_dict(1)
        t, s_t = generate_time_data_from_header(header_dictionary, num_messages)
        value_data = generate_value_data_from_header(header_dictionary, num_messages)
        print(header_dictionary)
        print(t)
        print(s_t)
        if header_dictionary['mode'] == ValueMode.TIME_VALUE_PAIRS.value:
            print(value_data)
        else:
            print(value_data[0])
        print()
