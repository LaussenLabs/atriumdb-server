import ctypes


class WALHeaderStructure(ctypes.Structure):
    _pack_ = 1
    _fields_ = [("version", ctypes.c_uint8),
                ("device_name", ctypes.c_char * 64),
                ("sample_freq", ctypes.c_uint64),
                ("input_value_type", ctypes.c_uint8),
                ("true_value_type", ctypes.c_uint8),
                ("mode", ctypes.c_uint8),
                ("samples_per_message", ctypes.c_uint32),
                ("file_start_time", ctypes.c_int64),
                ("scale_type", ctypes.c_uint8),
                ("scale_0", ctypes.c_double),
                ("scale_1", ctypes.c_double),
                ("scale_2", ctypes.c_double),
                ("scale_3", ctypes.c_double),
                ("measure_name", ctypes.c_char * 64),
                ("measure_units", ctypes.c_char * 64)]



header_attribute_list = [var_name for var_name, var_type in WALHeaderStructure._fields_]


def get_wal_header_struct_size():
    return ctypes.sizeof(WALHeaderStructure)


def get_header_structure_from_dict(header_dict):
    result = WALHeaderStructure()

    for key, value in header_dict.items():
        setattr(result, key, value)

    return result


if __name__ == "__main__":
    print(get_wal_header_struct_size())
