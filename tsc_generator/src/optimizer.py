import math
import os
import logging
import time
import xxhash
from config import config
from atriumdb import AtriumSDK, adb_functions
from helpers import sql_functions


_LOGGER = logging.getLogger(__name__)
sdk = None


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
    _LOGGER.info(f"Finding small tsc files took {time.perf_counter()-tik} s, for device_id={device_id}, measure_id={measure_id}")

    # if there is only one tsc file then don't optimize since there is only one partly full tsc file
    num_tsc_fles = len(set([block[3] for block in block_list]))
    if num_tsc_fles < 2:
        return 0

    tik = time.perf_counter()
    # get the min start time and max end time of the blocks so you only checksum the data you need to
    start_time, end_time = min(block[6] for block in block_list), max(block[7] for block in block_list)
    _LOGGER.info(f"Finding start and end times took {time.perf_counter() - tik} s, for device_id={device_id}, measure_id={measure_id}")

    tik = time.perf_counter()
    # checksum data to ensure before and after data are the same
    times_before_checksum, values_before_checksum = checksum_data(sdk=sdk, device_id=device_id, measure_id=measure_id,
                                                                  start_time=start_time, end_time=end_time)
    _LOGGER.info(f"Check summing took {time.perf_counter() - tik} s, for device_id={device_id}, measure_id={measure_id}")

    _LOGGER.info(f"Merging {num_tsc_fles} tsc files for device_id={device_id}, measure_id={measure_id}")

    tik = time.perf_counter()
    # figure out the parameters needed to merge smaller tsc files into bigger ones
    new_block_batches, old_block_batch_slices = make_optimal_tsc_files(device_id, measure_id, block_list)
    _LOGGER.info(f"Creating block bahces took {time.perf_counter() - tik} s, for device_id={device_id}, measure_id={measure_id}")

    # needed if the error happens during writing the files so the undo_changes doesn't fail with filenames being none
    filenames = []
    try:
        tik = time.perf_counter()
        for block_batch_idxs in old_block_batch_slices:
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
        sql_functions.insert_optimized_tsc_block_data(sdk, filenames, new_block_batches, blocks_old=block_list)

        _LOGGER.info(f"Merging tsc files took {time.perf_counter()-tik} s, for device_id={device_id}, measure_id={measure_id}")

        # confirm old and new tsc files have the same data
        times_after_checksum, values_after_checksum = checksum_data(sdk=sdk, device_id=device_id, measure_id=measure_id,
                                                                    start_time=start_time, end_time=end_time)
        # make sure checksums match
        assert times_before_checksum == times_after_checksum
        assert values_before_checksum == values_after_checksum

    # If checksums do not equal each other or there is another error undo the changes
    except AssertionError as e:
        _LOGGER.error("Checksums do not match restoring old blocks and deleting new ones", exc_info=True, stack_info=True)
        sql_functions.undo_changes(sdk, filenames, original_block_list=block_list)
    except Exception as e:
        _LOGGER.error("Error occurred while adding new data to the database restoring old blocks and deleting new ones", exc_info=True, stack_info=True)
        sql_functions.undo_changes(sdk, filenames, original_block_list=block_list)

    _LOGGER.info(f"Finished merging tsc files for device_id={device_id}, measure_id={measure_id}")


def make_optimal_tsc_files(device_id, measure_id, block_list):
    start_byte, end_byte, block_start, start_byte_array, new_block_batches, old_block_batch_slices = 0, 0, 0, [], [], []

    for i, block in enumerate(block_list):
        # here we are cutting the bytes stream into >= the target file size
        # if we go over the target tsc file size that's okay, but we must not be under the target size or the
        # optimizer will try to optimize this file again on the next optimizer run
        if end_byte - start_byte >= config.svc_tsc_gen['target_tsc_file_size'] or i+1 == len(block_list):

            # once the number of bytes threshold has been reached slice the blocks needed out of the block_list
            if i+1 != len(block_list):
                blocks = block_list[block_start:i]
                # append the numbers needed to slice the old block list later for reading the encoded bytes in batches
                old_block_batch_slices.append((block_start, i))
            else:
                # if we have reached the end of the block list make sure to include the last block
                blocks = block_list[block_start:]
                # append the numbers needed to slice the old block list later for reading the encoded bytes in batches
                old_block_batch_slices.append((block_start, len(block_list)))

                start_byte_array.append(end_byte - start_byte)
                end_byte += block[5]  # add num_bytes from the block to the end byte

            # Gather block and file data for insertion into mariadb later
            new_block_batches.append([(measure_id, device_id, int(start_byte_array[i]), b[5], b[6], b[7], b[8]) for i, b in enumerate(blocks)])

            # set the start block to the current iteration
            block_start = i
            # set the new start byte to the current end byte
            start_byte = end_byte
            # reset the start bytes list
            start_byte_array = []

        # append the starting byte of the block to the list for later insertion
        start_byte_array.append(end_byte-start_byte)

        # add the number of bytes in the block to the running total
        end_byte += block[5]

    return new_block_batches, old_block_batch_slices


# This function is used to confirm that the data before the optimization is the same as the data after the optimization
def checksum_data(sdk, device_id, measure_id, start_time, end_time):

    freq_nhz = sdk.get_measure_info(measure_id)['freq_nhz']
    # find time chunk size for hashing
    time_chunk_size = (config.svc_tsc_gen['num_blocks_checksum'] * config.svc_tsc_gen['optimal_block_num_values']) * ((10**18) / freq_nhz)
    num_chunks = math.ceil((end_time - start_time) / time_chunk_size)

    times_hash, values_hash = xxhash.xxh3_128(), xxhash.xxh3_128()
    for i in range(num_chunks):

        _, times, values = sdk.get_data(measure_id=measure_id, start_time_n=start_time + (time_chunk_size * i),
                                        end_time_n=start_time + (time_chunk_size * (i+1)), device_id=device_id,
                                        sort=True, allow_duplicates=True)

        if times.size != 0:
            times_hash.update(times) if times.dtype == 'int64' else times_hash.update(times.data)
            values_hash.update(values.data)

    return times_hash.hexdigest(), values_hash.hexdigest()


def delete_unreferenced_tsc_files(sdk):
    _LOGGER.info("Starting removal of unreferenced tsc files")
    # find tsc files in the file_index that have no references to them in the block_index
    files = sql_functions.find_unreferenced_tsc_files(sdk)

    if len(files) == 0:
        _LOGGER.info("No unreferenced tsc files to remove")
        return
    # remove them from the file_index
    sql_functions.delete_tsc_files(sdk, files)

    # extract file names from files and make it a set so we can do a set intersection later
    file_names = {file[1] for file in files}

    # walk the tsc directory looking for files to delete
    for root, _, files in os.walk(sdk.file_api.top_level_dir):
        # check if there is a match between any of the tsc file names to be deleted and files in the current directory
        matches = set(files) & file_names
        # if you find a match remove the file from disk
        if len(matches) > 0:
            for m in matches:
                _LOGGER.info(f"Deleting tsc file {m} from disk")
                os.remove(os.path.join(root, m))
    _LOGGER.info("Completed removal of unreferenced tsc files")
