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
from typing import Optional
from pydantic import BaseModel


class Patient(BaseModel):
    id: int
    mrn: int
    gender: Optional[str] = None
    dob: int
    first_name: Optional[str] = None  # needs to be optional because there's one patient in the database with no first name
    middle_name: Optional[str] = None
    last_name: Optional[str] = None
    first_seen: Optional[int] = None
    last_updated: Optional[int] = None
    source_id: Optional[int] = None
    height: Optional[float] = None
    height_units: Optional[str] = None
    height_time: Optional[int] = None
    weight: Optional[float] = None
    weight_units: Optional[str] = None
    weight_time: Optional[int] = None
