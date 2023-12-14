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