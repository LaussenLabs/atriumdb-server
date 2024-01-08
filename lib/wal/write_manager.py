from wal.manager import WALManager
from wal.io.reader import WALReader


class WALWriteManager(WALManager):
    def __init__(self, num_threads, directory):
        super().__init__(num_threads, directory)

    def _add_reader(self, thread_id, wal_reader: WALReader):
        pass
