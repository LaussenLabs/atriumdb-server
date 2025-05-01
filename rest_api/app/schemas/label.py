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
from pydantic import BaseModel
from typing import Optional, List, Tuple


class LabelName(BaseModel):
    id: int
    name: str
    parent_id: Optional[int] = None
    parent_name: Optional[str] = None


class LabelSource(BaseModel):
    id: int
    name: str
    description: Optional[str] = None


class Label(BaseModel):
    label_entry_id: int
    label_name_id: int
    label_name: str
    requested_name_id: Optional[int] = None
    requested_name: Optional[str] = None
    device_id: int
    device_tag: str
    patient_id: Optional[int] = None
    mrn: Optional[int] = None
    start_time_n: int
    end_time_n: int
    label_source_id: Optional[int] = None
    label_source: Optional[str] = None
    measure_id: Optional[int] = None


class LabelsQuery(BaseModel):
    label_name_id_list: Optional[List[int]] = None
    name_list: Optional[List[str]] = None
    device_list: Optional[List[int | str]] = None
    patient_id_list: Optional[List[int]] = None
    start_time: Optional[int] = None
    end_time: Optional[int] = None
    time_units: Optional[str] = None
    include_descendants: Optional[bool] = True
    limit: Optional[int] = 1000
    offset: Optional[int] = 0
    measure_list: Optional[List[int | Tuple[str, int | float, str]]] = None

