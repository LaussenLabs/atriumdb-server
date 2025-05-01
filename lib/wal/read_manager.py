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
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
import os.path

from wal.batch import WALBatch
from wal.read_process import read_batch


class WALReadManager:
    def __init__(self, directory, ingest_function, wait_close_time_s=None, max_workers=None):
        self.directory = os.path.abspath(directory)
        self.ingest_function = ingest_function
        self.p_exec = ProcessPoolExecutor(max_workers=max_workers)

        self.open_batches = {}
        self.closed_batches = {}

        self.wait_close_time_s = wait_close_time_s

        self.path_iter = []

    def loop_once(self, *args, sleep_time=None, delete_on_ingest=True):
        self.refresh_path_list()
        self.update_batches()
        self.queue_closed_batches(*args, delete_on_ingest=delete_on_ingest)
        self.clean_ingested_batches()

        if sleep_time is not None:
            time.sleep(sleep_time)

    def refresh_path_list(self):
        self.path_iter = Path(self.directory).glob("*.wal")

    def update_batches(self):
        for path in self.path_iter:
            file_hash = get_file_hash_from_path(path)

            if file_hash in self.closed_batches:
                # Data from this hash is currently being processed
                # so wait until it's complete.
                continue

            elif file_hash in self.open_batches:
                # Add to batch (duplicates overwritten).
                self.open_batches[file_hash].add(path)

            else:
                # If it doesn't exist, start a new batch
                self.open_batches[file_hash] = WALBatch.from_path_list(
                    [path], wait_close_time_s=self.wait_close_time_s, header_hash=file_hash)

    def queue_closed_batches(self, *args, delete_on_ingest=True):
        for batch_hash, batch in self.open_batches.copy().items():
            if batch.is_ready():
                self.closed_batches[batch_hash] = self.p_exec.submit(
                    read_batch,
                    self.open_batches.pop(batch_hash),
                    self.ingest_function,
                    *args,
                    delete_on_ingest=delete_on_ingest)

    def clean_ingested_batches(self):
        for batch_hash, future in self.closed_batches.copy().items():
            if future.done():
                self.closed_batches.pop(batch_hash).result()

    def get_num_unfinished_batches(self):
        return len(self.closed_batches)

    def get_num_open_batches(self):
        return len(self.open_batches)


def get_file_hash_from_path(path: Path):
    return path.name.split('-')[0]
