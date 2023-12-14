from pathlib import Path
import time
from config import config


def get_file_iter(path_directory, wait_close_time_s=None):
    if wait_close_time_s is None:
        wait_close_time_s = config.svc_tsc_gen['default_wait_close_time']
    path_iter = Path(path_directory).glob("*.wal")
    current_time = time.time()

    for path in path_iter:
        if (current_time - path.stat().st_mtime) > wait_close_time_s:
            yield path
