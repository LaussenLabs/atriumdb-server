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
import logging
from atriumdb import AtriumSDK
from config import config
from read_wal import read_wal_file
from write_tsc import write_wal_data_to_sdk

_LOGGER = logging.getLogger(__name__)
atrium_sdk = None


def tsc_generator_process(wal_path, device_measure):
    global atrium_sdk

    if atrium_sdk is None:
        atrium_sdk = AtriumSDK(dataset_location=config.dataset_location, metadata_connection_type=config.svc_tsc_gen['metadb_connection']['type'],
                               connection_params=config.CONNECTION_PARAMS, num_threads=config.svc_tsc_gen['num_compression_threads'])
        atrium_sdk.block.block_size = config.svc_tsc_gen['optimal_block_num_values']

    wal_data = read_wal_file(wal_path)

    # if the wal file is empty just remove it
    if wal_data is None:
        response = 2
        _LOGGER.info(f"{str(wal_path)} too small to ingest.")
        wal_path.unlink()
    else:
        try:
            response = write_wal_data_to_sdk(wal_data, atrium_sdk)
        except Exception:
            response = -2
            _LOGGER.error(f"Error occurred while trying to save WAL file {str(wal_path)} to AtriumDB", exc_info=True, stack_info=True)

    if response == 0:
        wal_path.unlink()
        _LOGGER.debug(f"Successfully saved data to AtriumDB, deleting WAL file: {str(wal_path)}")

    # If duplicate data was detected delete the wall file
    elif response == 1:
        wal_path.unlink()

    return response, device_measure
