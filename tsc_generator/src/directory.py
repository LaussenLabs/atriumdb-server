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
