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
