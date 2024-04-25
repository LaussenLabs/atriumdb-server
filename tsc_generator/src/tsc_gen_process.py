import logging
from atriumdb import AtriumSDK
from config import config
from read_wal import read_wal_file
from write_tsc import write_wal_data_to_sdk

_LOGGER = logging.getLogger(__name__)
atrium_sdk = None


def tsc_generator_process(wal_path):
    global atrium_sdk

    if atrium_sdk is None:
        atrium_sdk = AtriumSDK(dataset_location=config.dataset_location, metadata_connection_type=config.svc_tsc_gen['metadb_connection']['type'],
                               connection_params=config.CONNECTION_PARAMS, num_threads=config.svc_tsc_gen['num_compression_threads'])
        atrium_sdk.block.block_size = config.svc_tsc_gen['optimal_block_num_values']

    wal_data = read_wal_file(wal_path)

    if wal_data is None:
        response = 2
        _LOGGER.info(f"{str(wal_path)} too small to ingest.")
        wal_path.unlink()
    else:
        response = write_wal_data_to_sdk(wal_data, atrium_sdk)

    if response == 0:
        wal_path.unlink()
        _LOGGER.info(f"Successfully saved data to AtriumDB, deleting WAL file: {str(wal_path)}")

    # If duplicate data was detected delete the wall file
    elif response == 1:
        wal_path.unlink()

    return response
