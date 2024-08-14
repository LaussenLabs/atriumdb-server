import math
import logging
import time
import xxhash
import os
from config import config
from atriumdb import AtriumSDK, adb_functions
from helpers import sql_functions


# the max number of blocks to optimize for a run
MAX_BLOCKS_PER_RUN = 100_000
sdk = None

_LOGGER = logging.getLogger(__name__)


def merge_small_tsc_files(device_id, measure_id):
    global sdk

    if sdk is None:
        sdk = AtriumSDK(dataset_location=config.dataset_location, metadata_connection_type=config.svc_tsc_gen['metadb_connection']['type'],
                        connection_params=config.CONNECTION_PARAMS, num_threads=config.svc_tsc_gen['num_compression_threads'])
        sdk.block.block_size = config.svc_tsc_gen['optimal_block_num_values']

    tik = time.perf_counter()
    # get blocks from tsc files that are not big enough
    block_list = sql_functions.find_small_tsc_files(sdk=sdk, device_id=device_id, measure_id=measure_id,
                                                    target_tsc_file_size=config.svc_tsc_gen['target_tsc_file_size'])
    _LOGGER.info(f"Finding small tsc files took {time.perf_counter() - tik} s, for device_id={device_id}, measure_id={measure_id}")

    # if there is only one tsc file then don't optimize since there is only one partly full tsc file
    num_tsc_fles = len(set([block[3] for block in block_list]))
    if num_tsc_fles < 2:
        return 0

    # we need to make sure that 100_000 blocks is not smaller than the target_tsc_file_size because if it is than
    # the optimizer will never reach the desired file size and will get stuck optimizing the same data over and over
    bytes_total, idx = 0, 0
    for i, block in enumerate(block_list):
        bytes_total += block[5]

        # if the sum is greater than the file size than break
        if bytes_total > config.svc_tsc_gen['target_tsc_file_size']:
            idx = i
            break

    # if it took less than 100_000 blocks to fill a file than use the default of 100_000 blocks for the optimization
    # if it took more than however many it took will be used to slice the block list
    if idx < MAX_BLOCKS_PER_RUN:
        idx = MAX_BLOCKS_PER_RUN

    # limit the number of blocks that can be optimized during a single run
    block_list = block_list[:idx]

    tik = time.perf_counter()
    # checksum data to ensure before and after data are the same
    checksum_before = checksum_data(sdk, block_list)

    _LOGGER.info(f"Check summing took {time.perf_counter() - tik} s, for device_id={device_id}, measure_id={measure_id}")

    _LOGGER.info(f"Merging {num_tsc_fles} tsc files for device_id={device_id}, measure_id={measure_id}")

    # figure out the parameters needed to merge smaller tsc files into bigger ones
    start_byte_array, block_batch_slices = make_optimal_tsc_files(block_list)

    # needed if the error happens during writing the files so the undo_changes doesn't fail with filenames being none
    filenames = []
    try:
        tik = time.perf_counter()
        for block_batch_idxs in block_batch_slices:
            # make read list as small as possible to speed up process
            read_list = adb_functions.condense_byte_read_list(block_list[block_batch_idxs[0]:block_batch_idxs[1]])
            # extract file ids from condensed read list and get the tsc file names they map to
            file_id_list = [row[2] for row in read_list]
            filename_dict = sdk.get_filename_dict(file_id_list)

            # get encoded bytes from small tsc files
            encoded_bytes = sdk.file_api.read_file_list(read_list, filename_dict)
            # write the new tsc file to disk that contains the info
            filenames.append(sdk.file_api.write_bytes(measure_id, device_id, encoded_bytes))

        # insert the new filenames and their associated blocks. Then delete the old blocks in one transaction
        sql_functions.update_block_tsc_data(sdk, filenames, block_list, block_batch_slices, start_byte_array)

        _LOGGER.info(f"Merging tsc files took {time.perf_counter() - tik} s, for device_id={device_id}, measure_id={measure_id}")

        tik = time.perf_counter()
        # get the new blocks by using the tsc filenames
        new_blocks = sql_functions.select_blocks_by_file(sdk, filenames)
        # checksum the new blocks
        checksum_after = checksum_data(sdk, new_blocks)

        _LOGGER.info(f"Second check summing took {time.perf_counter() - tik} s, for device_id={device_id}, measure_id={measure_id}")
        # make sure checksums match
        assert checksum_after == checksum_before

    # If checksums do not equal each other or there is another error undo the changes
    except AssertionError as e:
        _LOGGER.error(f"Checksums do not match for device_id={device_id}, measure_id={measure_id}, restoring old blocks and deleting new ones", exc_info=True, stack_info=True)
        sql_functions.undo_changes(sdk, filenames, original_block_list=block_list)
    except Exception as e:
        _LOGGER.error(f"Error occurred while adding new data to the database for device_id={device_id}, measure_id={measure_id}, restoring old blocks and deleting new ones", exc_info=True, stack_info=True)
        sql_functions.undo_changes(sdk, filenames, original_block_list=block_list)

    _LOGGER.info(f"Finished merging tsc files for device_id={device_id}, measure_id={measure_id}")


def make_optimal_tsc_files(block_list):
    start_byte, end_byte, block_start, start_byte_array, block_batch_slices = 0, 0, 0, [], []

    for i, block in enumerate(block_list):

        start_byte_array.append(end_byte - start_byte)
        end_byte += block[5]

        # here we are cutting the bytes stream into >= the target file size
        # if we go over the target tsc file size that's okay, but we must not be under the target size or the
        # optimizer will try to optimize this file again on the next optimizer run
        if end_byte >= config.svc_tsc_gen['target_tsc_file_size'] or i + 1 == len(block_list):
            # append the numbers needed to slice the old block list later for reading the encoded bytes in batches
            block_batch_slices.append((block_start, i + 1))

            # set the start block to the current iteration
            block_start = i + 1
            start_byte, end_byte = 0, 0

    return start_byte_array, block_batch_slices


# This function is used to confirm that the data before the optimization is the same as the data after the optimization
def checksum_data(sdk, block_list):
    num_chunks = math.ceil(len(block_list) / config.svc_tsc_gen['num_blocks_checksum'])
    checksum = xxhash.xxh3_128()

    for i in range(num_chunks):
        # make read list as small as possible to speed up process
        read_list = adb_functions.condense_byte_read_list(block_list[i * config.svc_tsc_gen['num_blocks_checksum']:(i + 1) * config.svc_tsc_gen['num_blocks_checksum']])
        # extract file ids from condensed read list and get the tsc file names they map to
        file_id_list = [row[2] for row in read_list]
        filename_dict = sdk.get_filename_dict(file_id_list)
        # get encoded bytes from tsc files
        encoded_bytes = sdk.file_api.read_file_list(read_list, filename_dict)

        # update the checksum
        checksum.update(encoded_bytes)

    return checksum.hexdigest()


def delete_unreferenced_tsc_files(sdk):
    _LOGGER.info("Starting removal of unreferenced tsc files")

    # find tsc files in the file_index that have no references to them in the block_index
    files = sdk.sql_handler.find_unreferenced_tsc_files()

    # if there are no tsc files to remove just return
    if len(files) == 0:
        _LOGGER.info("No unreferenced tsc files to remove")
        return

    # extract file names from files and make it a set so we can do a set intersection later
    file_names = {file[1] for file in files}
    # extract the ids and put them in a tuple so we can remove them from the sql table later
    file_ids = [(file[0],) for file in files]

    # walk the tsc directory looking for files to delete (walk is a generator for memory efficiency)
    for root, dir_names, files in os.walk(sdk.file_api.top_level_dir):
        # remove any dirs that are not digits (device_id or measure_id) so walk doesn't traverse those directories
        dir_names[:] = [d for d in dir_names if d.isdigit()]

        # check if there is a match between any of the tsc file names to be deleted and files in the current directory
        matches = set(files) & file_names

        # if you find a match remove the file from disk
        for m in matches:
            print(f"Deleting tsc file {m} from disk")
            os.remove(os.path.join(root, m))

    # free up memory
    del file_names

    # remove them from the file_index
    sdk.sql_handler.delete_tsc_files(file_ids)
    print("Completed removal of unreferenced tsc files")