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
from enum import Enum


class ValueMode(Enum):
    TIME_VALUE_PAIRS = 0
    INTERVALS = 1

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


class ScaleType(Enum):
    NONE = 0
    LINEAR = 1


class ValueType(Enum):
    FLOAT32 = 0
    FLOAT64 = 1
    INT8 = 2
    INT16 = 3
    INT32 = 4
    INT64 = 5
