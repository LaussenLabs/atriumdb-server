from fastapi import APIRouter, Depends, Response, Security, HTTPException, WebSocket, WebSocketDisconnect, WebSocketException
from rest_api.app.core.auth import auth, Auth0User
from rest_api.app.core.atriumdb import atriumdb_sdk
from rest_api.app.core.config import settings
from atriumdb import adb_functions
from pathlib import Path
from typing import Optional, Annotated
from collections import Counter
from rest_api.app.core.authorization.dependencies import validate_token
import time

router = APIRouter()


class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def send(self, message: bytes, websocket: WebSocket):
        await websocket.send_bytes(message)


manager = ConnectionManager()


@router.get("/blocks", dependencies=[Depends(auth.implicit_scheme)])
async def get_blocks(
        start_time: int,
        end_time: int,
        measure_id: int,
        patient_id: Optional[int] = None,
        mrn: Optional[str] = None,
        device_id: Optional[int] = None,
        user: Auth0User = Security(auth.get_user)):
    # BASE VALIDATION
    if measure_id is None:
        raise HTTPException(status_code=400, detail="A measure_id must be specified")
    if sum(x is not None for x in (device_id, patient_id, mrn)) != 1:
        raise HTTPException(status_code=400, detail="Exactly one of device_id, patient_id, or mrn must be specified")
    if start_time > end_time:
        raise HTTPException(status_code=400, detail="The start time must be lower than the end time")

    if device_id is not None:
        device_info = atriumdb_sdk.get_device_info(device_id=device_id)

        # If the device_id doesn't exist in atriumdb raise an error
        if device_info is None:
            raise HTTPException(status_code=400, detail=f"Cannot find device_id={device_id} in AtriumDB")

    if patient_id is not None:
        patient_id_dict = atriumdb_sdk.get_patient_id_to_mrn_map([patient_id])
        if patient_id not in patient_id_dict:
            raise HTTPException(status_code=400, detail=f"Cannot find patient_id={patient_id} in AtriumDB.")

    if mrn is not None:
        mrn_patient_id_map = atriumdb_sdk.get_mrn_to_patient_id_map([int(mrn)])
        if mrn not in mrn_patient_id_map:
            raise HTTPException(status_code=400, detail=f"Cannot find mrn={mrn} in AtriumDB.")
        patient_id = mrn_patient_id_map[mrn]

    block_info = []
    block_id_list = atriumdb_sdk.sql_handler.select_blocks(
        measure_id, start_time_n=start_time, end_time_n=end_time,
        device_id=device_id, patient_id=patient_id)

    for row in block_id_list:
        block_info.append({'id': row[0],
                           'start_time_n': row[6],
                           'end_time_n': row[7],
                           'num_values': row[8],
                           'num_bytes': row[5]})
    return block_info


@router.get("/blocks/{block_id}", dependencies=[Depends(auth.implicit_scheme)])
async def get_block(block_id: int, user: Auth0User = Security(auth.get_user)):
    # Get block_info using block_id from sql table
    block_info = atriumdb_sdk.sql_handler.select_block(block_id=block_id)

    if block_info is None:
        raise HTTPException(status_code=404, detail=f"Cannot find block_id={block_id} in AtriumDB.")

    # extract the file_id from block info
    file_id_list = [block_info[3]]
    # get the name of the TSC file that contains the block
    filename = atriumdb_sdk.get_filename_dict(file_id_list)[block_info[3]]

    if not Path(filename).is_file():
        # get the absolute path to the TSC file containing the block
        filename = atriumdb_sdk.file_api.to_abs_path(filename, block_info[1], block_info[2])

    with open(filename, 'rb') as file:
        # Search the file to find the start byte from the block_info
        file.seek(block_info[4])
        # read from the start byte to the end byte in the file and return it
        return Response(file.read(block_info[5]))


@router.websocket("/blocks/ws")
async def websocket_endpoint(websocket: WebSocket, token: Annotated[dict, Depends(validate_token)]):
    # get the expiry date of the jwt
    token_exp = token['exp']

    await manager.connect(websocket)
    try:
        while True:
            # wait for the sdk to send the block_id its looking for
            block_ids = await websocket.receive_text()

            # check if the client token is expired
            if token_exp < time.time():
                # let the client know their token is expired and end the connection
                await websocket.send_text('expired_token')
                manager.disconnect(websocket)

            # split the comma delimited string into the block id's
            block_ids = block_ids.split(',')

            try:
                # Get block_info using block_ids from sql table
                block_list = atriumdb_sdk.sql_handler.select_blocks_by_ids(block_id_list=block_ids)
            except RuntimeError as e:
                # if not all block_ids are found in atriumdb a runtime error will be raised
                raise WebSocketException(code=1011, reason=f"{e}")

            # combine file reads for continuous blocks so you don't have to open files as much
            block_read_list = adb_functions.condense_byte_read_list(block_list)

            # get number of times each file needs to be read by seeing how many times a file_id appears in the read list
            file_read_counts = Counter([block[2] for block in block_read_list])

            # map the file-ids to the actual names of the files
            filename_dict = atriumdb_sdk.get_filename_dict(list(file_read_counts.keys()))

            # find which measure_id, device_id combo each file belongs too, by finding the first match in the block_list
            measure_device_ids = {file_id: next((block[1], block[2]) for block in block_list if block[3] == file_id) for file_id in list(file_read_counts.keys())}

            tsc_file_paths = {}
            # check to make sure the filenames exist on the server and if they do find the path
            for file_id, filename in filename_dict.items():
                if not Path(filename).is_file():
                    # get the absolute path to the TSC file containing the block
                    tsc_file_paths[file_id] = atriumdb_sdk.file_api.to_abs_path(filename, measure_device_ids[file_id][0], measure_device_ids[file_id][1])
                else:
                    raise WebSocketException(code=1011, reason=f"Path to {filename} not found.")

            # the max number of bytes we want to read at a time so we don't overwhelm the server
            max_read_bytes = settings.MAX_WEBSOCKET_FRAGMENT_SIZE
            total_num_bytes = sum([num_bytes for _, _, _, _, num_bytes in block_read_list])

            # if the number of bytes requested is less than the max read size make the buffer the size of the message
            if total_num_bytes <= max_read_bytes:
                # Create a bytearray with the total size of the data to be read
                message_bytes = bytearray(total_num_bytes)
            else:
                # if the total bytes to be read is larger than the max we can read at a time create a bytearray with
                # the max size of the data we can read at a time
                message_bytes = bytearray(max_read_bytes)

            # Create a memoryview of the bytearray
            mem_view = memoryview(message_bytes)

            # Initialize an index to keep track of the current position in the memoryview and an open_file_dict to keep
            # track of open files
            open_files, index = {}, 0

            try:
                # Iterate through the read_list and read the specified bytes from the files using read_into_bytearray
                for measure_id, device_id, file_id, start_byte, num_bytes in block_read_list:

                    # if we have not already opened the file open it and add it to the dictionary
                    if file_id not in open_files:
                        # Open the file in binary mode with the absolute path provided by the to_abs_path function
                        open_files[file_id] = open(tsc_file_paths[file_id], 'rb')

                    # get open file from dictionary
                    file = open_files[file_id]
                    # subtract 1 from the number of times the file needs to be read from
                    file_read_counts[file_id] -= 1

                    # Find the specified start byte position in the file
                    file.seek(start_byte)

                    # if there is still space in the message for the next iteration
                    if index+num_bytes < max_read_bytes:
                        # Read the data into the given bytearray object
                        file.readinto(mem_view[index:index + num_bytes])
                        # Update the index for the next iteration
                        index += num_bytes
                    else:
                        # Read the data into the given bytearray object
                        num_bytes_read = file.readinto(mem_view[index:])
                        # send the full message
                        await manager.send(message_bytes, websocket)

                        # reset index incase the message was filled perfectly
                        index = 0
                        num_bytes_left = num_bytes - num_bytes_read

                        # incase there is more than one message worth of bytes in this read
                        while num_bytes_left > 0:
                            # if there is more than a full message left read the next segment of bytes
                            if num_bytes_left > max_read_bytes:
                                num_bytes_read = file.readinto(mem_view)
                                await manager.send(message_bytes, websocket)
                                # calculate how many bytes are left to read
                                num_bytes_left -= num_bytes_read
                                index = 0
                            else:
                                # if there is <= one full message of bytes left read it into the buffer
                                num_bytes_read = file.readinto(mem_view[:num_bytes_left])
                                index += num_bytes_read
                                break

                    # check if the file read count is 0 and if it has, close the file and remove it from the dictionary
                    if file_read_counts[file_id] == 0:
                        open_files[file_id].close()
                        del open_files[file_id]

            # if there is an error in the program make sure to close all open files
            finally:
                [open_files[file_id].close() for file_id, open_file in open_files.items()]

            # if the last message wasn't filled send only the bytes in the buffer that were filled
            if index != 0:
                await manager.send(message_bytes[:index], websocket)

            # send the message that lets the sdk know that we are finished sending
            await websocket.send_text('Atriumdb_Done')

    except WebSocketDisconnect:
        manager.disconnect(websocket)
