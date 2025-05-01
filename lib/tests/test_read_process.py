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
import unittest
import numpy as np

from wal import WALBatch
from wal.io.enums import ValueMode
from wal.io.writer import WALWriter
from tests.wal_data_generator import generate_wal_data_arr, write_wal_data
from wal.read_process import io_read_batch, read_batch


class TestReadProcess(unittest.TestCase):

    def test_io_read_batch(self):
        # Set the initial variables
        batch_size = 10
        num_batches = 10
        wal_arr_size = num_batches * batch_size

        num_messages = 10 ** 3

        for enum_mode in list(ValueMode):
            mode = enum_mode.value

            # Generate the Data
            wal_data_arr = generate_wal_data_arr(mode, num_messages, wal_arr_size)
            wal_filenames = []

            # Write All Wal Data
            for wal_i, wal_data in np.ndenumerate(wal_data_arr):
                filename = "{}.wal".format(int(wal_i[0]))
                writer = WALWriter('.', filename)
                wal_filenames.append(writer.filename)
                writer.write_wal_data(wal_data)
                writer.close()

            for batch_i in range(num_batches):
                # Create Batch Object
                batch = WALBatch.from_path_list(wal_filenames[batch_i * batch_size: (batch_i + 1) * batch_size])

                # Read all data in 1 batch
                batch_wal_data = io_read_batch(batch.get_paths())

                # Delete batches files
                batch.delete_all_paths()

                # Check equality
                for inner_batch_i, wal_data_read in enumerate(batch_wal_data):
                    wal_i = (batch_i * batch_size) + inner_batch_i
                    self.assertTrue(wal_data_read == wal_data_arr[wal_i])

    def test_total_read_process(self):
        wal_arr_size = 10

        num_messages = 10 ** 5

        num_write_threads = 10

        # Define mock ingest function
        total_reads = 0

        def ingest_fn(ingested_wal_data):
            nonlocal total_reads
            if ingested_wal_data is not None:
                total_reads += 1
            return ingested_wal_data

        for enum_mode in list(ValueMode):
            mode = enum_mode.value

            # Reset total reads
            total_reads = 0

            # Write the Data to disk
            batch_filenames, wal_data_arr = write_wal_data(
                'test_data', mode, num_messages, wal_arr_size, files_per_batch=num_write_threads)

            for wal_data, file_batch in zip(wal_data_arr, batch_filenames):
                # Create Batch Object
                batch = WALBatch.from_path_list(file_batch)

                # Run Read Process Code
                finished_batch = read_batch(batch, ingest_fn, delete_on_ingest=True)

                self.assertTrue(finished_batch is batch)

                # Test Equality
                self.assertTrue(finished_batch.result == wal_data)

            # Test Total Reads
            self.assertTrue(total_reads == wal_arr_size)


if __name__ == '__main__':
    unittest.main()
