from enum import Enum

class DatabaseUpdateType(Enum):
    SET = 0
    INCREMENT = 1
    DECREMENT = 2
