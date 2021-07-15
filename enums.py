from enum import Enum

class DatabaseUpdateType(Enum):
    SET = 0
    INCREMENT = 1
    DECREMENT = 2

class SortBy(Enum):
    ASCENDING = 1
    DESCENDING = 0