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
import shutil
from pathlib import Path
import os


def reset_atriumdb_database(highest_level_dir):
    db_path = f"{highest_level_dir}/meta/index.db"
    tsc_path = f"{highest_level_dir}/tsc"
    wal_path = f"{highest_level_dir}/wal"

    Path(db_path).unlink(missing_ok=True)

    if Path(tsc_path).is_dir():
        shutil.rmtree(tsc_path)
    os.mkdir(highest_level_dir + "/tsc")

    if Path(wal_path).is_dir():
        shutil.rmtree(wal_path)
    os.mkdir(highest_level_dir + "/wal")

def main():

    DATASET_DIR = "C:/Users/spencer vecile/OneDrive - SickKids/Documents/datasets/test_dataset/"

    #reset the dataset
    reset_atriumdb_database(DATASET_DIR)

if __name__ == "__main__":
    main()