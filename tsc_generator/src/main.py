import sys
import signal
import time
import numpy as np
import datetime as dt
from atriumdb import AtriumSDK
from wal import WALHeaderStructure
from helpers import sql_functions
from directory import get_file_iter
from tsc_gen_process import tsc_generator_process
from optimizer import delete_unreferenced_tsc_files, merge_small_tsc_files
from config import config
from threading import Event
from logging import getLogger, Formatter, StreamHandler
from concurrent.futures import ProcessPoolExecutor, TimeoutError
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
    counter_dict = {-2: get_metric(TSCGENERATOR_ERRORS),
                    -1: get_metric(TSCGENERATOR_CORRUPTED_WAL_FILE),
                    0: get_metric(TSCGENERATOR_PROCESSED_WAL_FILE),
                    1: get_metric(TSCGENERATOR_DUPLICATE_WAL_FILE),
                    2: get_metric(TSCGENERATOR_WAL_FILE_EMPTY)}

    _LOGGER.info("TSC generator started")

    # need this since if the optimizer finishes within an hour of starting it may try run again
    opt_ran_today = False

    with ProcessPoolExecutor(max_workers=config.svc_tsc_gen['max_workers']) as executor:
        # this will contain the measure device combinations that are currently being ingested
        locked_device_measures = set()

        while not EXIT_EVENT.is_set():
            futures = []
            file_iter = get_file_iter(config.svc_wal_writer['wal_folder_path'])

            for wal_path in file_iter:
                # decode the wal header only so we can get device and measure information
                wal_header = WALHeaderStructure.from_buffer(np.fromfile(wal_path, dtype=np.uint8))

                # extract the measure and device information from the header then make it into a tuple
                device_measure = (wal_header.device_name.decode('utf-8'), wal_header.measure_name.decode('utf-8'),
                                  wal_header.sample_freq, wal_header.measure_units.decode('utf-8'))

                # if a wal file containing this measure device combination is not being already ingested then ingest it
                # This is to avoid a race condition in the block merging code where if two processes try to work on the
                # the same measure device combo they could both read the block information perform their respective merges
                # then add two blocks back to the block index where only one should be (creating a ton of duplication)
                if device_measure not in locked_device_measures:
                    # lock this measure device combo by adding it to the locked set
                    locked_device_measures.add(device_measure)

                    future = executor.submit(tsc_generator_process, wal_path, device_measure)
                    futures.append(future)

            for future in futures:
                try:
                    response_code, device_measure = future.result(timeout=config.svc_tsc_gen['wal_file_timeout'])
                    # use dictionary to avoid large if-else block
                    counter_dict[response_code].add(1)

                    # remove the measure device combo from the locked set so other wal file with this combo can be ingested
                    locked_device_measures.remove(device_measure)

                    # if there is some kind of error saving the data to atriumdb exit the program
                    if response_code == -2:
                        EXIT_EVENT.set()

                except TimeoutError:
                    if config.loglevel.upper() == "DEBUG":
                        _LOGGER.error("Timeout occurred while working on WAL file. If the keeps happening consider making the wal_file_timeout variable larger.", stack_info=True, exc_info=True)
                    else:
                        _LOGGER.error("Timeout occurred while working on WAL file. If the keeps happening consider making the wal_file_timeout variable larger.")

                    counter_dict[-2].add(1)
                    EXIT_EVENT.set()

            # If there are no wal files that need to be ingested wait before rechecking
            if len(futures) == 0:
                EXIT_EVENT.wait(config.svc_tsc_gen['wait_recheck_time'])

            # check if it's time to run the tsc file optimizer
            if not opt_ran_today and dt.datetime.now().hour == config.svc_tsc_gen['tsc_optimizer_run_time']:
                futures = []
                sdk = AtriumSDK(dataset_location=config.dataset_location,
                                metadata_connection_type=config.svc_tsc_gen['metadb_connection']['type'],
                                connection_params=config.CONNECTION_PARAMS)

                tik = time.perf_counter()
                _LOGGER.info("Starting TSC file optimization. Finding device measures with small tsc...")
                # find the measure device combinations that have undersized tsc files
                device_measures_small_tsc = sql_functions.find_devices_measures_with_small_tsc_files(sdk, config.svc_tsc_gen['target_tsc_file_size'])
                _LOGGER.debug(f"Finding device measures with small tsc files took {time.perf_counter() - tik} s")

                if len(device_measures_small_tsc) != 0:
                    _LOGGER.info("Starting tsc file size optimization")
                    start_bench = time.perf_counter()

                    for measure_id, device_id in device_measures_small_tsc:
                        future = executor.submit(merge_small_tsc_files, device_id, measure_id)
                        futures.append(future)

                    # wait for all the futures to complete
                    for future in futures:
                        try:
                            future.result(timeout=config.svc_tsc_gen['tsc_file_optimization_timeout'])
                        except TimeoutError:
                            if config.loglevel.upper() == "DEBUG":
                                _LOGGER.error(f"Timeout occurred while optimizing tsc files. If the keeps happening consider making the tsc_file_optimization_timeout variable larger.", stack_info=True, exc_info=True)
                            else:
                                _LOGGER.error(f"Timeout occurred while optimizing tsc files. If the keeps happening consider making the tsc_file_optimization_timeout variable larger.")

                            EXIT_EVENT.set()

                    _LOGGER.info("Completed tsc file size optimization")
                    _LOGGER.info(f"Total time to rollup tsc files took {str(dt.timedelta(seconds=int(time.perf_counter() - start_bench)))} hh:mm:ss")
                else:
                    _LOGGER.info("All tsc file sizes are optimal. Skipping optimization for today")

                # delete all the old tsc files that were put into new bigger files
                delete_unreferenced_tsc_files(sdk)

                opt_ran_today = True
                sdk.close()
                _LOGGER.info("SDK successfully closed connections")

            # an hour before the optimizer is set to run reset the opt_ran_today variable so it will run again
            if opt_ran_today and (dt.datetime.now().hour == config.svc_tsc_gen['tsc_optimizer_run_time'] - 1 or
                                  (config.svc_tsc_gen['tsc_optimizer_run_time'] == 0 and dt.datetime.now().hour == 23)):
                opt_ran_today = False


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
