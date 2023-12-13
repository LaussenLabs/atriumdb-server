from wal import WALReader, WALHeaderStructure
import ctypes


def read_wal_file(wal_path):
    wal_data = WALReader(wal_path).read_all()
    if wal_data.byte_arr.size < ctypes.sizeof(WALHeaderStructure):
        return None
    wal_data.interpret_byte_array()
    return wal_data
