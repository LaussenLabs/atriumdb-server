from typing import List, Dict
import os


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
            os.remove(sdk.file_api.to_abs_path(file=file, measure_id=original_block_list[0][1], device_id=original_block_list[0][2]))

        # remove tsc files from the file index
        cursor.executemany("DELETE FROM file_index WHERE path = ?", filename_tuples)


def find_unreferenced_tsc_files(sdk):
    with sdk.sql_handler.connection() as (conn, cursor):
        cursor.execute("SELECT t1.* FROM file_index t1 LEFT JOIN (SELECT DISTINCT file_id FROM block_index) t2 "
                       "ON t1.id = t2.file_id WHERE t2.file_id IS NULL")
        return cursor.fetchall()


def find_devices_measures_with_small_tsc_files(sdk, target_tsc_file_size):
    with sdk.sql_handler.connection() as (conn, cursor):
        cursor.execute("SELECT device_id, measure_id FROM block_index WHERE file_id IN "
                       "(SELECT file_id FROM block_index GROUP BY file_id HAVING SUM(num_bytes) < ?)"
                       " GROUP BY device_id, measure_id HAVING COUNT(file_id) >= 2",
                       (target_tsc_file_size,))
        return cursor.fetchall()


def find_small_tsc_files(sdk, device_id, measure_id, target_tsc_file_size):
    with sdk.sql_handler.connection() as (conn, cursor):
        # order by start and end time so the blocks are rewritten in order
        cursor.execute("SELECT * FROM block_index WHERE device_id = ? AND measure_id = ? AND file_id IN "
                       "(SELECT file_id FROM block_index GROUP BY file_id HAVING SUM(num_bytes) < ?) "
                       "ORDER BY start_time_n, end_time_n ASC", (device_id, measure_id, target_tsc_file_size))
        return cursor.fetchall()


def insert_optimized_tsc_block_data(sdk, file_names: List[str], blocks_new: List[List[Dict]], blocks_old: List[Dict]):
    with sdk.sql_handler.connection(begin=True) as (conn, cursor):
        # iterate over each file name and the list of blocks that goes with it and insert them
        for file_name, blocks in zip(file_names, blocks_new):
            # insert file_path into file_index and get id
            cursor.execute("INSERT INTO file_index (path) VALUES (?);", (file_name,))
            file_id = cursor.lastrowid

            # insert into block_index
            block_tuples = [(block[0], block[1], file_id, block[2], block[3], block[4], block[5], block[6]) for block in
                            blocks]
            cursor.executemany("INSERT INTO block_index (measure_id, device_id, file_id, start_byte, num_bytes, start_time_n, end_time_n, num_values) "
                               "VALUES (?, ?, ?, ?, ?, ?, ?, ?);", block_tuples)

        cursor.executemany("DELETE FROM block_index WHERE id = ?;", [(row[0],) for row in blocks_old])


def delete_tsc_files(sdk, files_to_delete: List[List]):
    with sdk.sql_handler.connection(begin=True) as (conn, cursor):
        # delete old block data
        cursor.executemany("DELETE FROM file_index WHERE id = ?;", [(row[0],) for row in files_to_delete])