import math
from typing import List, Dict
import os


def find_unreferenced_tsc_files(sdk):
    with sdk.sql_handler.connection() as (conn, cursor):
        cursor.execute("SELECT t1.id, t1.path FROM file_index t1 LEFT JOIN (SELECT DISTINCT file_id FROM block_index) t2 "
                       "ON t1.id = t2.file_id WHERE t2.file_id IS NULL")
        return cursor.fetchall()


def find_devices_measures_with_small_tsc_files(sdk, target_tsc_file_size):
    with sdk.sql_handler.connection() as (conn, cursor):
        cursor.execute("SELECT bi1.measure_id, bi1.device_id FROM block_index bi1 JOIN "
                       "(SELECT file_id FROM block_index GROUP BY file_id HAVING SUM(num_bytes) < 100000000) bi2 "
                       "ON bi1.file_id = bi2.file_id GROUP BY bi1.measure_id, bi1.device_id HAVING COUNT(DISTINCT bi1.file_id) >= 2",
                       (target_tsc_file_size,))
        return cursor.fetchall()


def find_small_tsc_files(sdk, device_id, measure_id, target_tsc_file_size):
    with sdk.sql_handler.connection() as (conn, cursor):
        # order by start and end time so the blocks are rewritten in order
        cursor.execute("SELECT id, measure_id, device_id, file_id, start_byte, num_bytes, start_time_n, end_time_n, num_values"
                       " FROM block_index WHERE measure_id = ? AND device_id = ? AND file_id IN "
                       "(SELECT file_id FROM block_index WHERE measure_id = ? AND device_id = ? GROUP BY file_id HAVING SUM(num_bytes) < ?) "
                       "ORDER BY start_time_n ASC, end_time_n ASC", (measure_id, device_id, measure_id, device_id, target_tsc_file_size))
        return cursor.fetchall()


# this one is for when you update blocks
# def update_block_tsc_data(sdk, file_names: List[str], blocks_old: List[Dict], block_batch_slices, start_byte_array):
#     with sdk.sql_handler.connection(begin=True) as (conn, cursor):
#         for i, file_name in enumerate(file_names):
#             # insert file_path into file_index and get id
#             cursor.execute("INSERT INTO file_index (path) VALUES (?);", (file_name,))
#             file_id = cursor.lastrowid
#
#             blocks = blocks_old[block_batch_slices[i][0]:block_batch_slices[i][1]]
#             start_bytes = start_byte_array[block_batch_slices[i][0]:block_batch_slices[i][1]]
#
#             # update block_index
#             update_tuples = [(file_id, start_byte, block[0]) for start_byte, block in zip(start_bytes, blocks)]
#
#             cursor.executemany("UPDATE block_index SET file_id = ?, start_byte = ? WHERE id = ?;", update_tuples)


# this one is for when you delete blocks then insert new ones
def update_block_tsc_data(sdk, file_names: List[str], blocks_old: List[Dict], block_batch_slices, start_byte_array):
    with sdk.sql_handler.connection(begin=True) as (conn, cursor):
        for i, file_name in enumerate(file_names):
            # insert file_path into file_index and get id
            cursor.execute("INSERT INTO file_index (path) VALUES (?);", (file_name,))
            file_id = cursor.lastrowid

            blocks = blocks_old[block_batch_slices[i][0]:block_batch_slices[i][1]]
            start_bytes = start_byte_array[block_batch_slices[i][0]:block_batch_slices[i][1]]

            # insert into block_index
            block_tuples = [(block[1], block[2], file_id, start_byte, block[5], block[6], block[7], block[8]) for start_byte, block in zip(start_bytes, blocks)]

            cursor.executemany("INSERT INTO block_index (measure_id, device_id, file_id, start_byte, num_bytes, start_time_n, end_time_n, num_values) "
                               "VALUES (?, ?, ?, ?, ?, ?, ?, ?);", block_tuples)

        cursor.executemany("DELETE FROM block_index WHERE id = ?;", [(row[0],) for row in blocks_old])


def select_blocks_by_file(sdk, file_names: List[str]):
    with sdk.sql_handler.connection(begin=False) as (conn, cursor):
        # use the file names to get the file_ids
        cursor.execute("SELECT id FROM file_index WHERE path IN ({})".format(','.join(['?'] * len(file_names))), tuple(file_names))
        file_ids = cursor.fetchall()

        cursor.execute("SELECT id, measure_id, device_id, file_id, start_byte, num_bytes, start_time_n, end_time_n, num_values"
                       " FROM block_index WHERE file_id IN ({}) ORDER BY start_time_n ASC, end_time_n ASC".format(','.join(['?'] * len(file_ids))), tuple([id[0] for id in file_ids]))

        return cursor.fetchall()


def delete_tsc_files(sdk, file_ids_to_delete: List[tuple]):
    with sdk.sql_handler.connection(begin=False) as (conn, cursor):

        # if you put too many rows in the delete statement mariadb will fail. So we split it up
        for i in range(math.ceil(len(file_ids_to_delete)/100_000)):
            # delete old tsc files
            cursor.executemany("DELETE FROM file_index WHERE id = ?;", file_ids_to_delete[i*100_000:(i+1)*100_000])


# this one is for when you update blocks
# def undo_changes(sdk, filename_list, original_block_list):
#     # not doing it in a transaction because if it doesn't get a chance to reinsert all of them, i want as many as
#     # possible. Its also faster so less chance of it being interrupted
#     with sdk.sql_handler.connection(begin=False) as (conn, cursor):
#         # update block_index
#         update_tuples = [(block[3], block[4], block[0]) for block in original_block_list]
#
#         cursor.executemany("UPDATE block_index SET file_id = ?, start_byte = ? WHERE id = ?;", update_tuples)
#
#         # delete the tsc files from disk
#         for file in filename_list:
#             os.remove(sdk.file_api.to_abs_path(filename=file, measure_id=original_block_list[0][1], device_id=original_block_list[0][2]))
#
#         filename_tuples = [(row,) for row in filename_list]
#
#         # remove tsc files from the file index
#         cursor.executemany("DELETE FROM file_index WHERE path = ?", filename_tuples)


# this one is for when you delete blocks then insert new ones
def undo_changes(sdk, filename_list, original_block_list):
    # not doing it in a transaction because if it doesn't get a chance to reinsert all of them, i want as many as
    # possible. Its also faster so less chance of it being interrupted
    with sdk.sql_handler.connection(begin=False) as (conn, cursor):
        # reinsert the original blocks that were deleted to the block index. The blocks may not have been deleted yet so use insert ignore
        cursor.executemany("INSERT IGNORE INTO block_index (id, measure_id, device_id, file_id, start_byte, num_bytes, start_time_n, end_time_n, num_values) "
                           "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);", original_block_list)

        # delete the new optimized blocks that were added
        filename_tuples = [(row,) for row in filename_list]
        cursor.executemany("DELETE FROM block_index WHERE file_id = (SELECT id FROM file_index WHERE path = ?)", filename_tuples)

        # delete the tsc files from disk
        for file in filename_list:
            os.remove(sdk.file_api.to_abs_path(filename=file, measure_id=original_block_list[0][1], device_id=original_block_list[0][2]))

        # remove tsc files from the file index
        cursor.executemany("DELETE FROM file_index WHERE path = ?", filename_tuples)