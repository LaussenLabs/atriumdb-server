import time
from concurrent.futures import ProcessPoolExecutor, as_completed
import logging
from atriumdb import AtriumSDK
from directory import get_file_iter
from tsc_gen_process import tsc_generator_process
from config import config
from helpers.metrics import (get_metric,
                             TSCGENERATOR_ERRORS,
                             TSCGENERATOR_PROCESSED_WAL_FILE,
                             TSCGENERATOR_WAL_FILE_EMPTY,
                             TSCGENERATOR_DUPLICATE_WAL_FILE)

_LOGGER = logging.getLogger(__name__)


def run_tsc_generator():
    if config.svc_tsc_gen['create_dataset']:
        # This is here to instantiate the dataset if it dones not already exist to avoid a race condition between the
        # workers as they start up and all try to make a new dataset at the same time
        AtriumSDK.create_dataset(dataset_location=config.dataset_location, database_type=config.svc_tsc_gen['metadb_connection']['type'],
                                 connection_params=config.CONNECTION_PARAMS, overwrite='ignore')

    # Set up open telemetry metrics
    errors_counter = get_metric(TSCGENERATOR_ERRORS)
    counter_dict = {0: get_metric(TSCGENERATOR_PROCESSED_WAL_FILE),
                    1: get_metric(TSCGENERATOR_DUPLICATE_WAL_FILE),
                    2: get_metric(TSCGENERATOR_WAL_FILE_EMPTY)}

    while True:
        with ProcessPoolExecutor(max_workers=config.svc_tsc_gen['max_workers']) as executor:
            ingest_futures = []
            file_iter = directoryget_file_iter(config.svc_wal_writer['wal_folder_path'])

            for wal_path in file_iter:
                future = executor.submit(tsc_generator_process, wal_path)
                ingest_futures.append(future)

            for future in as_completed(ingest_futures):
                try:
                    # use dictionary to avoid large if-else block
                    counter_dict[future.result()].add(1)
                except Exception as e:
                    _LOGGER.error("Error occurred with worker while working on WAL file {}.".format(wal_path), exc_info=True)
                    errors_counter.add(1)
                    raise e

            # If there are no wal files that need to be ingested sleep before rechecking
            if len(ingest_futures) == 0:
                time.sleep(config.svc_tsc_gen['wait_recheck_time'])
