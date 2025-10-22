from datetime import datetime
from typing import TypedDict

class Record(TypedDict):
    value: str
    expire_at: datetime
class StreamRecord(TypedDict):
    id: str
    data: dict[str, str]
class NullArray:
    pass

class ErrorResponse():
    def __init__(self, message: str):
        self.message = message