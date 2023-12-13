from pathlib import Path
import time
from typing import List


DEFAULT_WAIT_CLOSE_TIME = 60 * 5  # 5 minutes


class WALBatch:
    def __init__(self, wait_close_time_s=None, header_hash=None):
        self.paths = []
        self.wait_close_time_s = wait_close_time_s if wait_close_time_s is not None else DEFAULT_WAIT_CLOSE_TIME
        self.header_hash = header_hash
        self.result = None

    @classmethod
    def from_path_list(cls, path_list: List[Path], wait_close_time_s=None, header_hash=None):
        result = cls(wait_close_time_s=wait_close_time_s, header_hash=header_hash)
        [result.add(path) for path in path_list]
        return result

    def __contains__(self, item):
        return Path(item) in self.paths

    def __len__(self):
        return len(self.paths)

    def get_paths(self):
        return self.paths

    def add(self, path):
        path = Path(path)
        if path not in self:
            self.paths.append(path)

    def is_ready(self):
        # If empty, there's no point
        if len(self.paths) == 0:
            return False

        # Get the current time
        current_time = time.time()
        for path in self.paths:
            if (current_time - path.stat().st_mtime) < self.wait_close_time_s:
                # If one of the files has been modified recently, we're not ready
                return False
        return True

    def delete_all_paths(self):
        [p.unlink() for p in self.paths]
