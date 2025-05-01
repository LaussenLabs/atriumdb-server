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
        atexit.register(self.close)

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
        # all files within an hour of mtime go in the same file
        header['file_start_time'] = (int((data_time_ns / 1_000_000_000) - (
                    (data_time_ns / 1_000_000_000) % self.file_length_time))) * 1_000_000_000

        header["measure_name"] = bytes(measure_name+("\0" * (64 - len(measure_name))), 'utf-8')
        header["measure_units"] = bytes(measure_units+("\0" * (64 - len(measure_units))), 'utf-8')
        header["true_value_type"] = ValueType.FLOAT64.value  # not used
        # default values
        values = 0
        header["scale_type"] = ScaleType.NONE.value
        header["scale_0"] = float(0)
        header["scale_1"] = float(0)
        # these are for possible expansion and currently are unused
        header["scale_2"] = float(0)
        header["scale_3"] = float(0)

        if msg_type =="wav":
            header["mode"] = ValueMode.INTERVALS.value
            header["samples_per_message"] = 0  # not used for waveforms
            # parse the values from the '^' delimited string
            values = np.fromstring(data, dtype=float, sep='^')

            # check for scale factors for Phillips and Draeger data
            if meta_data is not None and "scale_m" in meta_data and "scale_b" in meta_data:

                # if m or b are not 0 the values need to be scaled
                if meta_data["scale_m"] != 0 or meta_data["scale_b"] != 0:
                    # Set scale type to linear (this is not used now but may be in the future)
                    header["scale_type"] = ScaleType.LINEAR.value
                    # store scale factors so we can convert back to floats later when the data is decompressed
                    header["scale_0"] = meta_data["scale_b"]
                    header["scale_1"] = meta_data["scale_m"]

                    # convert to ints (for better compression) using scale factors
                    values = ((values - meta_data["scale_b"]) / meta_data["scale_m"])

                # type cast to integers (if values wern't scaled that means they were already ints)
                values = np.rint(values).astype(np.dtype("<i8"))

                # this is the type we are going to write to the wal files
                # type is now INT because we converted to it using scale factors or they were just ints to start with
                header["input_value_type"] = ValueType.INT64.value

            # here no scaling is applied and floats are written to disk
            else:
                header["input_value_type"] = ValueType.FLOAT64.value

        # metrics dont get scaled
        elif msg_type == "met":
            header["mode"] = ValueMode.TIME_VALUE_PAIRS.value
            header["samples_per_message"] = 1
            header["input_value_type"] = ValueType.FLOAT64.value
            values = float(data)

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
        self._LOGGER.debug("Running GC")
        with self.lock:
            keys = tuple(self.pool.keys())
            for key in keys:
                self.pool[key]["handle"].flush()
                if time.time() - self.pool[key]["last_access"] >= self.idle_timeout:
                    self._LOGGER.debug("Closing: {}".format(self.pool[key]["file_path"]))
                    self.pool[key]["handle"].close()
                    del self.pool[key]
                    self.open_wal_file_counter.add(-1)

    # when the wal writer exits this will close all open files and shut down the scheduler
    def close(self):
        if self.scheduler.running:
            self.scheduler.shutdown()

        if len(self.pool) != 0:
            self._LOGGER.info("Closing open WAL files...")
            with self.lock:
                keys = tuple(self.pool.keys())
                for key in keys:
                    self.pool[key]["handle"].flush()
                    self._LOGGER.info("Closing: {}".format(self.pool[key]["file_path"]))
                    self.pool[key]["handle"].close()
                    del self.pool[key]
                    self.open_wal_file_counter.add(-1)
            self._LOGGER.info("All WAL files closed")
