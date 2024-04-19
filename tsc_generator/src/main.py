import sys
import signal
from atriumdb import AtriumSDK
from directory import get_file_iter
from tsc_gen_process import tsc_generator_process
from config import config
from threading import Event
from logging import getLogger, Formatter, StreamHandler
from concurrent.futures import ProcessPoolExecutor, as_completed
from helpers.metrics import (get_metric,
                             TSCGENERATOR_ERRORS,
                             TSCGENERATOR_CORRUPTED_WAL_FILE,
                             TSCGENERATOR_PROCESSED_WAL_FILE,
                             TSCGENERATOR_WAL_FILE_EMPTY,
                             TSCGENERATOR_DUPLICATE_WAL_FILE)

# stop event for graceful exit of the tsc generator
EXIT_EVENT = Event()

# set up logging
_LOGGER = getLogger()
fmt = Formatter(fmt="%(asctime)s  %(name)s - %(levelname)s %(threadName)s - %(message)s")
handler = StreamHandler()
handler.setFormatter(fmt)
_LOGGER.addHandler(handler)
_LOGGER.setLevel(config.loglevel.upper())


def run_tsc_generator():
    if config.svc_tsc_gen['create_dataset']:
        # This is here to instantiate the dataset if it dones not already exist to avoid a race condition between the
        # workers as they start up and all try to make a new dataset at the same time
        AtriumSDK.create_dataset(dataset_location=config.dataset_location,
                                 database_type=config.svc_tsc_gen['metadb_connection']['type'],
                                 connection_params=config.CONNECTION_PARAMS, overwrite='ignore')

    # Set up open telemetry metrics
    errors_counter = get_metric(TSCGENERATOR_ERRORS)
    counter_dict = {-2: get_metric(TSCGENERATOR_ERRORS),
                    -1: get_metric(TSCGENERATOR_CORRUPTED_WAL_FILE),
                    0: get_metric(TSCGENERATOR_PROCESSED_WAL_FILE),
                    1: get_metric(TSCGENERATOR_DUPLICATE_WAL_FILE),
                    2: get_metric(TSCGENERATOR_WAL_FILE_EMPTY)}

    _LOGGER.info("TSC generator started")

    with ProcessPoolExecutor(max_workers=config.svc_tsc_gen['max_workers']) as executor:
        while not EXIT_EVENT.is_set():
            ingest_futures = []
            file_iter = get_file_iter(config.svc_wal_writer['wal_folder_path'])

            for wal_path in file_iter:
                future = executor.submit(tsc_generator_process, wal_path)
                ingest_futures.append(future)

            for future in ingest_futures:
                try:
                    as_completed([future], timeout=config.svc_tsc_gen['wal_file_timeout'])

                    # use dictionary to avoid large if-else block
                    counter_dict[future.result()].add(1)

                except Exception as e:
                    _LOGGER.error("Error occurred with worker while working on WAL file {}.".format(wal_path), exc_info=True)
                    errors_counter.add(1)
                    EXIT_EVENT.set()
                    raise e

            # If there are no wal files that need to be ingested wait before rechecking
            if len(ingest_futures) == 0:
                EXIT_EVENT.wait(config.svc_tsc_gen['wait_recheck_time'])


def signal_handler(signo, _frame):
    sig_lookup = {1: "HUP", 2: "INT", 15: "TERM"}
    sig = sig_lookup.get(signo, "unknown")
    _LOGGER.exception("Interrupted by %s %d, in Frame %s", sig, signo, _frame)
    EXIT_EVENT.set()


if __name__ == "__main__":
    # set up signals for graceful exit
    for sig in ('TERM', 'HUP', 'INT'):
        # windows can only handle a few signals and the ones we care about are SIGTERM and SIGINT so skip the rest
        if (sig != 'TERM' or sig != 'INT') and sys.platform == "win32":
            continue
        signal.signal(getattr(signal, 'SIG' + sig), signal_handler)

    run_tsc_generator()
