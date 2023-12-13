import os
import time
import unittest
from pathlib import Path

from wal.io.enums import ValueMode
from wal.read_manager import WALReadManager, get_file_hash_from_path
from tests.wal_data_generator import write_wal_data


def ingest_fn(ingested_wal_data):
    if ingested_wal_data is not None:
        print(ingested_wal_data)

    return ingested_wal_data


class TestReadManager(unittest.TestCase):

    def test_loop_manager(self):
        wal_arr_size = 10

        num_messages = 10 ** 3

        # num_write_threads = 1
        for num_write_threads in [1, 2]:

            wait_time = 2  # 2 seconds
            # mode = ValueMode.INTERVALS.value
            for mode_enum in list(ValueMode):
                mode = mode_enum.value

                test_dir = 'test_data'

                manager = WALReadManager(test_dir, ingest_fn, wait_close_time_s=wait_time)

                batch_filenames, wal_data_arr = write_wal_data(
                    manager.directory, mode, num_messages, wal_arr_size, files_per_batch=num_write_threads)

                manager.loop_once()

                while manager.get_num_open_batches() > 0 or manager.get_num_unfinished_batches() > 0:
                    self.assertTrue(set(manager.open_batches.keys()).isdisjoint(set(manager.closed_batches.keys())))
                    manager.loop_once(sleep_time=0.1)

                for batch_f in batch_filenames:
                    batch_hash = get_file_hash_from_path(Path(batch_f[0]))
                    self.assertTrue(batch_hash not in manager.closed_batches)
                    self.assertTrue(batch_hash not in manager.open_batches)

    def test_read_file_list(self):
        wal_arr_size = 4

        num_messages = 10 ** 5

        num_write_threads = 2
        mode = ValueMode.INTERVALS.value

        test_dir = 'test_data'

        batch_filenames, manager, wal_data_arr = self._setup_batches(
            mode, num_messages, num_write_threads, wal_arr_size, WALReadManager(test_dir, ingest_fn))

        [os.remove(filename) for batch in batch_filenames for filename in batch]

    def test_manager(self):
        wal_arr_size = 10

        num_messages = 10 ** 5

        num_write_threads = 2

        wait_time = 2  # 2 seconds
        mode = ValueMode.INTERVALS.value

        test_dir = 'test_data'

        manager = WALReadManager(test_dir, ingest_fn, wait_close_time_s=wait_time)

        batch_filenames, wal_data_arr = write_wal_data(
            manager.directory, mode, num_messages, wal_arr_size, files_per_batch=num_write_threads)

        manager.refresh_path_list()
        manager.update_batches()

        self._assert_batches_in_open_batches(batch_filenames, manager)

        manager.queue_closed_batches()

        if manager.get_num_open_batches() == wal_arr_size:
            self._assert_batches_in_open_batches(batch_filenames, manager)

        while manager.get_num_open_batches() > 0:
            time.sleep(0.1)
            manager.queue_closed_batches()

        for batch_f in batch_filenames:
            batch_hash = get_file_hash_from_path(Path(batch_f[0]))
            self.assertTrue(batch_hash in manager.closed_batches)
            self.assertTrue(batch_hash not in manager.open_batches)

        while manager.get_num_unfinished_batches() > 0:
            time.sleep(0.1)
            manager.clean_ingested_batches()

        for batch_f in batch_filenames:
            batch_hash = get_file_hash_from_path(Path(batch_f[0]))
            self.assertTrue(batch_hash not in manager.closed_batches)
            self.assertTrue(batch_hash not in manager.open_batches)

    def _assert_batches_in_open_batches(self, batch_filenames, manager):
        for batch_paths in batch_filenames:
            batch_hash = get_file_hash_from_path(Path(batch_paths[0]))
            self.assertTrue(batch_hash in manager.open_batches)

            batch = manager.open_batches[batch_hash]

            self.assertTrue(set(batch.get_paths()) == set([Path(f) for f in batch_paths]))

    def _setup_batches(self, mode, num_messages, num_write_threads, wal_arr_size, manager):
        batch_filenames, wal_data_arr = write_wal_data(
            manager.directory, mode, num_messages, wal_arr_size, files_per_batch=num_write_threads)

        all_filenames = [Path(filename) for batch in batch_filenames for filename in batch]

        manager.refresh_path_list()
        read_path_names = [Path(read_path) for read_path in manager.path_iter]

        self.assertTrue(set(read_path_names) == set(all_filenames))

        return batch_filenames, manager, wal_data_arr


if __name__ == '__main__':
    unittest.main()
