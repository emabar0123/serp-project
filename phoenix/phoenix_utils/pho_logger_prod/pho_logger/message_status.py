from enum import Enum


class Status(Enum):
    Started = "Started"
    InProgress = "InProgress"
    Finished = "Finished"
    Failed = "Failed"
    Cancelled = "Cancelled"
    ReQueued = "ReQueued"
