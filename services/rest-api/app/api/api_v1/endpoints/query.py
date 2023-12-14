from app.core.auth import auth, Auth0User
from app.core.database import database
from app.core.siri import siri
from app.core.atriumdb import atriumdb_sdk
from app.core.config import settings
from typing import Optional
from fastapi import APIRouter, Depends, Security, HTTPException
import numpy as np
import time
# from app.core.influx import influx
# from io import BytesIO
# import pandas as pd

router = APIRouter()


@router.get("", dependencies=[Depends(auth.implicit_scheme)])
async def get_query(

        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        device_id: Optional[int] = None,
        measure_id: Optional[int] = None,

        device_tag: Optional[str] = None,
        measure_tag: Optional[str] = None,
        measure_units: Optional[str] = None,
        freq: Optional[int | float] = None,
        freq_units: Optional[str] = None,

        patient_id: Optional[int] = None,
        mrn: Optional[str] = None,
        time_precision: Optional[str] = None,
        user: Auth0User = Security(auth.get_user)
):
    # BASE VALIDATION
    await validate_base_params(measure_id, measure_tag, freq, measure_units, device_id, device_tag, patient_id, mrn,
                               start_time, end_time)

    start_time, end_time = await convert_time_to_nano(start_time, end_time, time_precision)

    freq_nhz, measure_id, measure_tag, measure_units = await get_measure_data(measure_id, measure_tag, freq, measure_units, freq_units)

    type = "metric"
    if freq_nhz > 1_000_000_000:
        type = "wave"

    # if the user only inputs an end time then make the start time the end time - the max time aloud per query
    if start_time is None and end_time is not None:
        if type == "wave":
            start_time = end_time - (settings.WAVES_MAX_TIME * 1_000_000_000)
        else:
            start_time = end_time - (settings.METRICS_MAX_TIME * 1_000_000_000)
    # if the user only inputs a start time then make the end time the end time + the max time aloud per query
    elif start_time is not None and end_time is None:
        if type == "wave":
            end_time = start_time + (settings.WAVES_MAX_TIME * 1_000_000_000)
        else:
            end_time = start_time + (settings.METRICS_MAX_TIME * 1_000_000_000)
    # if they input neither a start or end time then give them the max amount of the most recent data
    elif start_time is None and end_time is None:
        if type == "wave":
            end_time = int(time.time()) * 1_000_000_000
            start_time = end_time - (settings.WAVES_MAX_TIME * 1_000_000_000)
        else:
            end_time = int(time.time()) * 1_000_000_000
            start_time = end_time - (settings.METRICS_MAX_TIME * 1_000_000_000)

    if type == "wave" and (end_time - start_time) > (settings.WAVES_MAX_TIME * 1_000_000_000):
        raise HTTPException(status_code=400,
                            detail="A maximum of {} seconds of waveform data can be requested at a time".format(
                                settings.WAVES_MAX_TIME))
    elif type == "metric" and (end_time - start_time) > (settings.METRICS_MAX_TIME * 1_000_000_000):
        raise HTTPException(status_code=400,
                            detail="A maximum of {} seconds of metric data can be requested at a time".format(
                                settings.METRICS_MAX_TIME))

    # GET DEVICE DATA
    # If a user chooses to get devices from patient_id or mrn
    if patient_id is not None or mrn is not None:
        devices = await get_device_list(start_time, end_time, mrn, patient_id)

    # If they choose to use the device_id system to find the data
    elif device_id is not None:
        device_info = atriumdb_sdk.get_device_info(device_id=device_id)

        # If the device_id doesn't exist in atriumdb raise an error
        if device_info is None:
            raise HTTPException(status_code=404, detail="Cannot find device_id={} in AtriumDB".format(device_id))
        else:
            devices = [{"device_id": device_id, "start_time": start_time, "end_time": end_time}]

    # If a user decides to use the device_tag
    else:
        # get associated device id from device tag
        device_id = atriumdb_sdk.get_device_id(device_tag=device_tag)

        # if device_tag is not in atriumdb raise an error
        if device_id is None:
            raise HTTPException(status_code=404, detail="Cannot find device_tag={} in AtriumDB".format(device_tag))
        else:
            devices = [{"device_id": device_id, "start_time": start_time, "end_time": end_time}]

    times, values = [], []
    for device in devices:
        t, v = await get_device_data(type=type, device_id=device["device_id"], measure_id=measure_id,
                                     measure_tag=measure_tag, freq_nhz=freq_nhz, measure_units=measure_units,
                                     start_time=device["start_time"], end_time=device["end_time"])
        times.append(t)
        values.append(v)

    # if the query returns no results return the empty lists
    if len(times) == 0 and len(values) == 0:
        return {"devices": devices, "measure": measure_id, "times": times, "values": values}

    times, values = np.concatenate(times), np.concatenate(values)

    if time_precision is not None:
        times = convert_from_nanoseconds(time_array=times, time_precision=time_precision)

    return {"devices": devices, "measure": measure_id, "times": times.tolist(), "values": values.tolist()}


async def get_measure_data(measure_id, measure_tag, freq, measure_units, freq_units):
    # Set default value for freq_units to nano hertz
    freq_units = "nHz" if freq_units is None else freq_units
    # GET MEASURE DATA
    if measure_id is not None:
        # check if the measure_id is in atriumdb
        measure_info = atriumdb_sdk.get_measure_info(measure_id=measure_id)
        if measure_info is None:
            raise HTTPException(status_code=404, detail="Cannot find measure_id={} in AtriumDB".format(measure_id))
        else:
            freq_nhz = measure_info['freq_nhz']
            measure_tag = measure_info['tag']
            measure_units = measure_info['unit']

    # If using the tuple measure_tag, frequency and measure units get the associated measure_id
    else:
        freq_nhz = convert_to_nanohz(freq_nhz=freq, freq_units=freq_units)
        # get associated measure_id from measure_tag, frequency and measure units
        measure_id = atriumdb_sdk.get_measure_id(measure_tag=measure_tag, freq=freq_nhz, units=measure_units)

        # If atriumdb doesn't contain the tuple return an error
        if measure_id is None:
            raise HTTPException(status_code=404,
                                detail="No measure_id exists for the tuple measure_tag={}, measure_units={}, frequency={}".format(
                                    measure_tag, measure_units, freq))
    return freq_nhz, measure_id, measure_tag, measure_units


async def convert_time_to_nano(start_time, end_time, time_precision):
    if time_precision is not None:
        time_unit_options = {"ns": 1, "s": 10 ** 9, "ms": 10 ** 6, "us": 10 ** 3}
        # make sure a valid time precision has been entered
        if time_precision not in time_unit_options.keys():
            raise HTTPException(status_code=400, detail="Invalid time units. Expected one of: %s" % time_unit_options)

        # convert start and end time to nanoseconds
        if start_time is not None:
            start_time = int(start_time * time_unit_options[time_precision])
        if end_time is not None:
            end_time = int(end_time * time_unit_options[time_precision])
    return start_time, end_time


async def validate_base_params(measure_id, measure_tag, freq, measure_units, device_id, device_tag, patient_id, mrn,
                               start_time, end_time):
    if measure_id is None and (measure_tag is None or measure_units is None or freq is None):
        raise HTTPException(status_code=400,
                            detail="A measure_id or the tuple measure_tag, frequency and measure_units must be specified")
    if patient_id is None and mrn is None and device_tag is None and device_id is None:
        raise HTTPException(status_code=400,
                            detail="At least one of MRN, patient_id, device_id or device_tag must be specified")
    if measure_id is not None and (measure_tag is not None or measure_units is not None or freq is not None):
        raise HTTPException(status_code=400,
                            detail="Please only specify one of either the measure_id or the tuple measure_tag, frequency, and measure_unit")
    # If they have specified more than one of patient_id, mrn, device_id or device_tag
    if not ((device_id is not None) ^ (device_tag is not None) ^ (mrn is not None) ^ (patient_id is not None)):
        raise HTTPException(status_code=400,
                            detail="Please only specify one of patient_id, MRN, device_id or device_tag")
    if start_time is not None and end_time is not None and start_time > end_time:
        raise HTTPException(status_code=400, detail="The start time must be lower than the end time")


async def get_device_list(start_time, end_time, mrn=None, patient_id=None):
    devices = []

    if patient_id is not None:
        query = "SELECT de.device_id as device_id, de.start_time as start_time, if(de.end_time is null, :now, de.end_time) as end_time " \
                "FROM encounter e JOIN device_encounter de ON de.encounter_id=e.id " \
                "WHERE e.patient_id = :patient_id AND de.start_time <= :end_time AND " \
                ":start_time < if(de.end_time is null, :now, de.end_time)"

    elif mrn is not None:
        query = "SELECT de.device_id as device_id, de.start_time as start_time, if(de.end_time is null, :now, de.end_time) as end_time " \
                "FROM patient p JOIN encounter e ON e.patient_id=p.id JOIN device_encounter de ON de.encounter_id=e.id " \
                "WHERE p.mrn = :mrn AND de.start_time <= :end_time AND " \
                ":start_time < if(de.end_time is null, :now, de.end_time)"

    else:
        # Should never get here this would be an error
        return None

    identifier_type = "patient_id" if mrn is None else "mrn"
    identifier = patient_id if mrn is None else mrn

    res = await database.fetch_all(query=query, values=
    {
        identifier_type: identifier,
        "start_time": start_time,
        "end_time": end_time,
        "now": int(time.time()) * 1_000_000_000
    })

    for row in res:
        sTime = start_time
        eTime = end_time

        if start_time < row["start_time"]:
            sTime = row["start_time"]

        if end_time > row["end_time"]:
            eTime = row["end_time"]

        devices.append({"device_id": row["device_id"], "start_time": sTime, "end_time": eTime})

    return devices


async def get_device_data(type, device_id, measure_id, measure_tag, freq_nhz, measure_units, start_time, end_time):
    cur_time = time.time_ns()
    cross_over_ns = settings.XOVER_TIME * 3600 * 1_000_000_000

    x_time = cur_time - cross_over_ns

    # data is before cross over time
    if end_time <= x_time:
        _, times, values = atriumdb_sdk.get_data(measure_id=measure_id, device_id=device_id,
                                                         start_time_n=start_time, end_time_n=end_time)
    # data is after cross time
    elif start_time > x_time:
        times, values = await get_siri_data(type, device_id, measure_id, start_time, end_time)
        # times, values = get_influx_data(device_id, measure_tag, freq_nhz, measure_units, start_time, end_time)

    # data overlaps
    else:
        _, adb_times, adb_values = atriumdb_sdk.get_data(measure_id=measure_id, device_id=device_id,
                                                         start_time_n=start_time, end_time_n=x_time)
        siri_times, siri_values = await get_siri_data(type, device_id, measure_id, x_time, end_time)
        # influx_times, influx_values = get_influx_data(device_id, measure_tag, freq_nhz, measure_units, x_time, end_time)
        # times, values = np.concatenate([adb_times, influx_times]), np.concatenate([adb_values, influx_values])

        # Combine atriumdb and siridb data
        times, values = np.concatenate([adb_times, siri_times]), np.concatenate([adb_values, siri_values])

    return times, values


async def get_siri_data(type, device_id, measure_id, start_time, end_time):
    resp = await siri.query("select * from '{}-{}-{}' between {} and {}".format(type, device_id, measure_id,
                                                                                start_time, end_time))
    signals = list(resp.keys())
    if len(signals) == 0 or len(resp[signals[0]]) == 0:
        return np.array([]), np.array([])

    times, values = zip(*resp[signals[0]])

    return np.array(times, dtype="int64"), np.array(values)


# def get_influx_data(device_id, measure_tag, freq_nhz, measure_units, start_time, end_time):
#     freq = freq_nhz / (10 ** 9)
#     query_string = f'''from(bucket: "{settings.INFLUX_BUCKET}")
#     |> range(start: time(v: {start_time}), stop: time(v: {end_time}))
#     |> filter(fn: (r) => r["_measurement"] == "{measure_tag}")
#     |> filter(fn: (r) => r["_field"] == "val")
#     |> filter(fn: (r) => r["devid"] == "{device_id}")
#     |> filter(fn: (r) => r["freq"] == "{freq}")
#     |> filter(fn: (r) => r["uom"] == "{measure_units}")
#     |> keep(columns: ["_time", "_value"])'''
#     # '|> map(fn: (r) => ({ r with _time: uint(v: r._time)}))'
#
#     # use query raw because influx native parsing is very slow
#     response = influx.query_raw(query=query_string, dialect={'annotations': [], 'comment_prefix': '#', 'date_time_format': "RFC3339Nano", 'delimiter': ',', 'header': True})
#     try:
#         # parse the csv values into a dataframe and parse the times to pandas timestamp objects
#         df = pd.read_csv(BytesIO(response.data), usecols=['_time', '_value'], parse_dates=['_time'], date_format='ISO8601')
#     except pd.errors.EmptyDataError:
#         return np.array([]), np.array([])
#     # extract the nanosecond timestamp attribute from the series of pandas datetime objects
#     times = df['_time'].apply(lambda x: x.value).values
#     values = df['_value'].values
#
#     return times, values


def convert_to_nanohz(freq_nhz, freq_units):
    freq_units = "nHz" if freq_units is None else freq_units
    freq_unit_options = {"nHz": 1, "uHz": 10 ** 3, "mHz": 10 ** 6, "Hz": 10 ** 9, "kHz": 10 ** 12, "MHz": 10 ** 15}
    if freq_units not in freq_unit_options.keys():
        raise HTTPException(status_code=400, detail="Invalid frequency units. Expected one of: %s" % freq_unit_options)

    freq_nhz *= freq_unit_options[freq_units]

    return round(freq_nhz)


def convert_from_nanoseconds(time_array, time_precision):
    # check that a correct unit type was entered
    time_unit_options = {"ns": 1, "s": 10 ** 9, "ms": 10 ** 6, "us": 10 ** 3}

    if time_precision not in time_unit_options.keys():
        raise HTTPException(status_code=400, detail="Invalid time units. Expected one of: %s" % time_unit_options)

    # convert time data from nanoseconds to unit of choice
    time_array = time_array / time_unit_options[time_precision]

    if np.all(time_array == np.floor(time_array)):
        time_array = time_array.astype('int64')

    return time_array
