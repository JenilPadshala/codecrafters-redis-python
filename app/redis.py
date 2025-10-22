from datetime import datetime, timedelta, timezone
from typing import TypedDict, Any, Optional, Union
from collections import deque
from itertools import islice
import time
import asyncio
class Record(TypedDict):
    value: str
    expire_at: datetime

KVStore = dict[str, Record]
ListStore = dict[str, deque[Any]]
class Redis:
    def __init__(self) -> None:
        self.kv_store: KVStore = {}
        self.list_store: ListStore = {}
        self.epoch_zero = datetime.fromtimestamp(0, tz=timezone.utc)
        pass

    def handle_command(self, command: str, *args: Any):
        command = command.upper()
        print(f"Handling command: {command} with args: {args}")
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
        elif command == "LPUSH" and len(args) >= 2:
            return self.lpush(*args)
        elif command == "LLEN" and len(args) == 1:
            return self.llen(*args)
        elif command == "LPOP" and len(args) >= 1:
            return self.lpop(*args)
        elif command == "BLPOP" and len(args) >= 2:
            return self.blpop(*args)
    
    # ---- Command Implementations ----
    def ping(self) -> str:
        """Return PONG."""
        return "PONG"
    
    def echo(self, msg: str) -> str:
        """Return the same message back."""
        return msg
    
    def set(self, key: str, value: str, opt=None, expire_time=None) -> str:
        """Set the value of key with optional expiration."""
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
        """Get the value of key. If the key does not exist or is expired, return None."""
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
        """Append one or more values to the end of the list stored at key."""
        if key not in self.list_store:
            self.list_store[key] = deque()
        self.list_store[key].extend(values)
        print(self.list_store)
        return len(self.list_store[key])
    
    def lpush(self, key: str, *values: str) -> int:
        """Prepend one or more values to the beginning of the list stored at key."""
        if key not in self.list_store:
            self.list_store[key] = deque()
        self.list_store[key].extendleft(values)
        return len(self.list_store[key])
    
    def lrange(self, key: str, start: str, end: str) -> deque[str]:
        """Return the specified elements of the list stored at key."""
        if key not in self.list_store:
            return deque()
        lst = self.list_store[key]
        start_idx = int(start)
        end_idx = int(end)
        # Handle negative indices
        if start_idx < 0:
            start_idx += len(lst)
            start_idx = max(start_idx, 0)
        if end_idx < 0:
            end_idx += len(lst)
        # Adjust indices to be within bounds        
        if start_idx >= len(lst) or (start_idx > end_idx):
            return deque()
        if end_idx >= len(lst):
            end_idx = len(lst) - 1
        return deque(islice(lst, start_idx, end_idx + 1))
    
    def llen(self, key: str) -> int:
        """Return the length of the list stored at key."""
        if key not in self.list_store:
            return 0
        return len(self.list_store[key])
    
    def lpop(self, key: str, n: Optional[Union[int, str]]=None) -> Optional[Union[deque, list, str]]:
        """Remove and return the first 'n' elements of the list stored at key."""
        # if n is None, pop one element
        if not n:
            count = 1
        else:
            try:
                count = int(n)
            except ValueError:
                print("LPOP: n is not an integer")
                return None
        # if key does not exist or list is empty, return None
        if key not in self.list_store or len(self.list_store[key]) == 0:
            return None
        # if n is greater than or equal to the length of the list, return the whole list and empty it
        if count >= len(self.list_store[key]):
            lst = self.list_store[key]
            self.list_store[key] = deque()
            return lst
        # if n is 1, pop and return a single element
        elif count == 1:
            return self.list_store[key].popleft()
        
        # else, pop 'n' elements from the front and return them
        popped_elements = deque()
        for _ in range(count):
            popped_elements.append(self.list_store[key].popleft())
        return popped_elements
    
    def blpop(self, key: str, timeout: str):
        """Blocking LPOP"""
        try:
            timeout_sec = int(timeout)
        except ValueError:
            print("BLPOP: timeout is not an integer")
            return [None]
        timeout_limit = datetime.now(timezone.utc) + timedelta(seconds=timeout_sec)
        while True:
            print("BLPOP: checking list")
            if key in self.list_store and len(self.list_store[key])>0:
                return self.lpop(key)
            if timeout_sec > 0 and datetime.now(timezone.utc) >= timeout_limit:
                return [None]

            # Sleep for a short duration to avoid busy waiting
            asyncio.run(asyncio.sleep(0.1))
        