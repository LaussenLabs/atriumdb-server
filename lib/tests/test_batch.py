import unittest

from wal import WALBatch
from pathlib import Path
import time
import random


class TestBatch(unittest.TestCase):

    def test_all_batch(self):
        num_paths = 10
        wait_time_seconds = 2
        data_dir = "test_data"
        data_dir_path = Path(data_dir)

        # Make dir
        data_dir_path.mkdir(parents=True, exist_ok=True)

        file_paths = [Path(data_dir_path, "{}.data".format(i)) for i in range(num_paths)]

        batch = WALBatch(wait_close_time_s=wait_time_seconds)

        # Add the first 5
        [self._test_add_contains(p, batch) for p in file_paths[:num_paths // 2]]

        # Test contains
        [self._test_contains(p, batch) for p in file_paths[:num_paths // 2]]
        [self._test_not_contains(p, batch) for p in file_paths[num_paths // 2:]]

        # Test length
        self._test_len(batch, num_paths // 2)

        # Add all (duplicate paths should automatically be ignored)
        [self._test_add_contains(p, batch) for p in file_paths]

        # Test length
        self._test_len(batch, num_paths)

        # Test get paths
        self.assertTrue(set(batch.get_paths()) == set(file_paths))

        # Touch all
        [p.touch() for p in file_paths]

        # Test not read
        self._test_not_ready(batch)

        # Wait the required time + 10%
        time.sleep(wait_time_seconds * 1.1)

        # Test Ready
        self._test_ready(batch)

        # Touch one file
        random.choice(file_paths).touch()

        # Test not read
        self._test_not_ready(batch)

        # Wait the required time + 10%
        time.sleep(wait_time_seconds * 1.1)

        # Test Ready
        self._test_ready(batch)

        # Delete all
        batch.delete_all_paths()

        # Test delete
        [self.assertTrue(not p.exists()) for p in file_paths]

    def _test_add_contains(self, path: Path, batch: WALBatch):
        batch.add(path)
        self._test_contains(path, batch)

    def _test_contains(self, path: Path, batch: WALBatch):
        self.assertTrue(path in batch)

    def _test_not_contains(self, path: Path, batch: WALBatch):
        self.assertTrue(path not in batch)

    def _test_ready(self, batch: WALBatch):
        self.assertTrue(batch.is_ready())

    def _test_not_ready(self, batch: WALBatch):
        self.assertTrue(not batch.is_ready())

    def _test_len(self, batch: WALBatch, length: int):
        self.assertTrue(len(batch) == length)


if __name__ == '__main__':
    unittest.main()