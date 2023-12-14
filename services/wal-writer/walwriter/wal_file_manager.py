import orjson
import numpy as np
import atexit
import random
import time
import threading
import logging
import xxhash
from apscheduler.schedulers.background import BackgroundScheduler
from wal import WALWriter, ValueType, ValueMode, ScaleType, get_null_header_dictionary
from helpers.metrics import get_metric, WALWRITER_WAL_FILES_OPEN, WALWRITER_WAL_FILES_CREATED


class WALFileManager:

    pool = {}
    lock = threading.Lock()

    def __init__(self, path: str, file_length_time: int, idle_timeout: int, gc_schedule_min: int):

        self._LOGGER = logging.getLogger(__name__)
        self.path = path
        self.idle_timeout = idle_timeout
        self.file_length_time = file_length_time
        self.scheduler = BackgroundScheduler(daemon=True)
        self.scheduler.add_job(func=self._gc, trigger="interval", minutes=gc_schedule_min)
        self.scheduler.start()
        self.open_wal_file_counter = get_metric(WALWRITER_WAL_FILES_OPEN)  # open telemetry metric
        self.wal_files_created_counter = get_metric(WALWRITER_WAL_FILES_CREATED)
        atexit.register(lambda: self.scheduler.shutdown())

    # writes data to the appropriate file
    def write(self, device_name: str,  server_time_ns: int, msg_type: str, measure_name: str, data_time_ns: int,
              measure_units: str, freq: float, data: str, meta_data: dict = None):

        header, values = self.parse_header(device_name=device_name, msg_type=msg_type, measure_name=measure_name,
                                           data_time_ns=data_time_ns, measure_units=measure_units, freq=freq, data=data,
                                           meta_data=meta_data)

        with self.lock:
            file = self._get_file(meta_data=header)
            if header["mode"] == ValueMode.INTERVALS.value:
                file.write_interval_message(start_time_nominal=int(data_time_ns), start_time_server=int(server_time_ns),
                                            values=values)
            else:
                file.write_time_value_pair_message(time_nominal=int(data_time_ns), time_server=int(server_time_ns),
                                                   value=values)

    def parse_header(self, device_name: str, msg_type: str, measure_name: str, data_time_ns: int, measure_units: str,
                     freq: float, data: str, meta_data: dict = None):

        header = self.get_base_header()
        header["version"] = 1
        header["device_name"] = bytes(device_name+("\0"*(64-len(device_name))), 'utf-8')
        header["sample_freq"] = int(freq * (10 ** 9))
        # all files within an hour of mtime and
        header['file_start_time'] = (int((data_time_ns / 1_000_000_000) - (
                    (data_time_ns / 1_000_000_000) % self.file_length_time))) * 1_000_000_000

        header["measure_name"] = bytes(measure_name+("\0" * (64 - len(measure_name))), 'utf-8')
        header["measure_units"] = bytes(measure_units+("\0" * (64 - len(measure_units))), 'utf-8')
        # default for values
        values = 0

        if msg_type =="wav":
            header["mode"] = ValueMode.INTERVALS.value
            header["input_value_type"] = ValueType.INT16.value
            header["true_value_type"] = ValueType.FLOAT64.value
            values = np.fromstring(data, dtype=float, sep='^')
            values = ((values - meta_data["scale_b"]) / meta_data["scale_m"])  # convert to ints
            values = np.rint(values).astype(np.dtype("<i2"))
            header["samples_per_message"] = values.size
            header["scale_type"] = ScaleType.LINEAR.value
            header["scale_0"] = meta_data["scale_b"]
            header["scale_1"] = meta_data["scale_m"]
            header["scale_2"] = float(0)
            header["scale_3"] = float(0)

        elif msg_type == "met":
            header["input_value_type"] = ValueType.FLOAT64.value
            header["true_value_type"] = ValueType.FLOAT64.value
            header["mode"] = ValueMode.TIME_VALUE_PAIRS.value
            header["samples_per_message"] = 1
            values = float(data)
            header["scale_type"] = ScaleType.NONE.value
            header["scale_0"] = float(0)
            header["scale_1"] = float(0)
            header["scale_2"] = float(0)
            header["scale_3"] = float(0)

        return header, values

    def get_base_header(self):
        header = get_null_header_dictionary()
        header["version"] = 1
        return header

    # gets file from the pool, creating one if it doesn't exist
    def _get_file(self, meta_data: dict):
        key = self._get_key(meta_data)
        if key not in self.pool.keys():
            self._create_and_register(meta_data)

        self.pool[key]["last_access"] = time.time()
        return self.pool[key]["handle"]

    def _hash_metadata(self, meta_data: dict):
        # convert any values that are bytes in the dictionary to string so it can serialize
        meta = {k: v.decode('utf-8') if type(v) == bytes else v for k, v in meta_data.items()}

        return xxhash.xxh3_128(orjson.dumps(meta, option=orjson.OPT_SORT_KEYS)).hexdigest()

    # generates the name of the files
    def _get_file_name(self, meta_data: dict) -> str:
        return "{}-{}.wal".format(self._hash_metadata(meta_data), int(random.getrandbits(64)))

    # creates file key - used to lookup files in the map
    def _get_key(self, meta_data) -> str:
        return "{}".format(self._hash_metadata(meta_data))

    # creates a few file and registers it into the connection pool
    def _create_and_register(self, meta_data: dict):
        key = self._get_key(meta_data=meta_data)
        file_name = self._get_file_name(meta_data=meta_data)
        writer = WALWriter(directory=self.path, filename=file_name)
        writer.write_header(meta_data)
        entry = {
            "file_name": file_name,
            "file_path": '/'.join((self.path, file_name)),
            "handle": writer,
            "last_access": time.time()
        }
        self.pool[key] = entry
        self.open_wal_file_counter.add(1)
        self.wal_files_created_counter.add(1)

    # garbage collect stale file handles
    def _gc(self):
        self._LOGGER.info("Running GC")
        with self.lock:
            keys = tuple(self.pool.keys())
            for key in keys:
                self.pool[key]["handle"].flush()
                if time.time() - self.pool[key]["last_access"] >= self.idle_timeout:
                    self._LOGGER.info("Closing: {}".format(self.pool[key]["file_path"]))
                    self.pool[key]["handle"].close()
                    del self.pool[key]
                    self.open_wal_file_counter.add(-1)
