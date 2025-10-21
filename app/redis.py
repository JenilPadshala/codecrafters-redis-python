from datetime import datetime, timedelta, timezone
from typing import TypedDict, Any, Optional

class Record(TypedDict):
    value: str
    expire_at: datetime

KVStore = dict[str, Record]
ListStore = dict[str, list[Any]]
class Redis:
    def __init__(self) -> None:
        self.kv_store: KVStore = {}
        self.list_store: ListStore = {}
        self.epoch_zero = datetime.fromtimestamp(0, tz=timezone.utc)
        pass

    def handle_command(self, command: str, *args: str):
        command = command.upper()
        if command == "PING":
            return self.ping()
        elif command == "ECHO" and args:
            return self.echo(args[0])
        elif command == "SET" and len(args) >= 2:
            return self.set(*args)
        elif command == "GET" and len(args) == 1:
            return self.get(args[0])
        elif command == "RPUSH" and len(args) >= 2:
            return self.rpush(*args)
        elif command == "LRANGE" and len(args) == 3:
            return self.lrange(*args)
    
    # ---- Command Implementations ----
    def ping(self) -> str:
        return "PONG"
    
    def echo(self, msg: str) -> str:
        return msg
    
    def set(self, key: str, value: str, opt=None, expire_time=None) -> str:
        if opt and expire_time:
            if opt.upper() == "EX":
                expire_time = int(expire_time) * 1000  # convert to milliseconds
            elif opt.upper() == "PX":
                expire_time = int(expire_time)
            expire_at = datetime.now(timezone.utc) + timedelta(milliseconds=expire_time)
        else:
            expire_at = self.epoch_zero  # no expiration
        self.kv_store[key] = {"value": value, "expire_at": expire_at}
        print(self.kv_store)
        return "OK"
    
    def get(self, key: str) -> Optional[str]:
        if key in self.kv_store:
            record = self.kv_store[key]
            expire_at = record["expire_at"]
            now = datetime.now(tz=timezone.utc)
            if expire_at == self.epoch_zero or (expire_at is not None and expire_at > now):
                return record["value"]
            else:
                del self.kv_store[key]
                return None
        else:
            return None
    
    def rpush(self, key: str, *values: str) -> int:
        if key not in self.list_store:
            self.list_store[key] = []
        self.list_store[key].extend(values)
        return len(self.list_store[key])
    
    def lrange(self, key: str, start: str, end: str) -> list[str]:
        if key not in self.list_store:
            return []
        lst = self.list_store[key]
        start_idx = int(start)
        end_idx = int(end)
        # Handle negative indices
        if start_idx < 0:
            start_idx += len(lst)
        if end_idx < 0:
            end_idx += len(lst)
        
        if start_idx >= len(lst) or (start_idx > end_idx):
            return []
        if end_idx >= len(lst):
            end_idx = len(lst) - 1
        return lst[start_idx:end_idx + 1]
        
