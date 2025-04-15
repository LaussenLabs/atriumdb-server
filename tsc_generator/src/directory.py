from pathlib import Path
import time
from config import config


def get_file_iter(path_directory, wait_close_time_s=None):
    if wait_close_time_s is None:
        wait_close_time_s = config.svc_tsc_gen['default_wait_close_time']
    path_iter = Path(path_directory).glob("*.wal")
    current_time = time.time()

    # sort paths by oldest modified to newest to make data ingestion happen in order
    sorted_paths = sorted(path_iter, key=lambda p: p.stat().st_mtime)

    for path in sorted_paths:
        if (current_time - path.stat().st_mtime) > wait_close_time_s:
            yield path
