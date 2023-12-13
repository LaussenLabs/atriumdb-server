import os.path

from wal.io.data import WALData


class WALReader:
    def __init__(self, path):
        # Represents 1 Wal File
        # Could be a function instead of a class,
        # but we may want to expand functionality later.
        self.path = os.path.abspath(path)

    def read_all(self):
        return WALData.from_file(self.path)
