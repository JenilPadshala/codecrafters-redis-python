from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Union, Dict
from collections import deque
from itertools import islice
import asyncio
from .custom_data_types import Record, NullArray, StreamRecord, ErrorResponse


KVStore = dict[str, Record]
ListStore = dict[str, deque[Any]]

class Redis:
    def __init__(self) -> None:
        # In-memory data stores
        self.kv_store: KVStore = {}
        self.list_store: ListStore = {}
        self.stream_store: Dict[str,deque[StreamRecord]] = {}

        self.epoch_zero = datetime.fromtimestamp(0, tz=timezone.utc)
        
        self.commands = {
            "PING": (self.ping, 0),
            "ECHO": (self.echo, 1),
            "SET": (self.set, 2),
            "GET": (self.get, 1),
            "RPUSH": (self.rpush, 2),
            "LRANGE": (self.lrange, 3),
            "LPUSH": (self.lpush, 2),
            "LLEN": (self.llen, 1),
            "LPOP": (self.lpop, 1),
            "BLPOP": (self.blpop, 2),
            "TYPE": (self.type, 1),
            "XADD": (self.xadd, 3),
            "XRANGE": (self.xrange, 3),
            "XREAD": (self.xread, 3),
        }

    async def handle_command(self, command: str, *args: Any):
        command = command.upper()
        print(f"Handling command: {command} with args: {args}")
        handler, num_args = self.commands.get(command, (None, None))

        if not handler:
            return None
        if len(args) < num_args:
            return None
        try:
            if asyncio.iscoroutinefunction(handler):
                return await handler(*args)
            else:
                return handler(*args)
        except TypeError as e:
            return None
    
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
    
    async def blpop(self, *args):
            """
            Blocking LPOP. Blocks until an element is available in one of the lists,
            or until the timeout is reached.
            """
            if len(args) < 2:
                # Not enough arguments (must have at least one key and a timeout)
                return NullArray()

            # The last argument is the timeout, all preceding are keys
            keys = args[:-1]
            timeout_str = args[-1]

            try:
                # Use float for more precise timeout, like real Redis
                timeout_sec = float(timeout_str)
            except ValueError:
                print("BLPOP: timeout is not a number")
                return NullArray()

            timeout_limit = datetime.now(timezone.utc) + timedelta(seconds=timeout_sec)

            while True:
                #Check all keys for an available item
                for key in keys:
                    if key in self.list_store and len(self.list_store[key]) > 0:
                        value = self.list_store[key].popleft()
                        return [key, value] 

                if timeout_sec != 0 and datetime.now(timezone.utc) >= timeout_limit:
                    return NullArray()
                await asyncio.sleep(0.01)
    
    def type(self, key: str) -> str:
        """Return the data type of the value stored at key."""
        if key in self.kv_store:
            return "string"
        elif key in self.list_store:
            return "list"
        elif key in self.stream_store:
            return "stream"
        else:
            return "none"
    
    def xadd(self, key: str, id: str, *field_value_pairs: str) -> Union[bytes, ErrorResponse]:
        """Append only stream entry to the stream at key."""
        # Parse/validate ID
        final_id = self._parse_stream_id(id, key)
        if isinstance(final_id, ErrorResponse):
            return final_id

        # Parse field-value pairs into a dictionary
        data = self._parse_stream_field_value_pairs(field_value_pairs)
        if isinstance(data, ErrorResponse):
            return data
        
        # If the stream does not exist, create it
        if key not in self.stream_store:
            self.stream_store[key] = deque()

        self.stream_store[key].append(StreamRecord(id=final_id, data=data))
        return final_id.encode()
    
    def xrange(self, key: str, start: str, end: str) -> Union[deque, NullArray]:
        """Return the stream entries in the specified ID range [start, end]."""
        if key not in self.stream_store:
            return NullArray()

        # parse start and end IDs
        start_id = (0, 0) if start == '-' else self._parse_id(start)
        end_id = (float('inf'), float('inf')) if end == '+' else self._parse_id(end)

        stream = self.stream_store[key]
        result = deque()
        # Iterate over stream and collect entries within range
        for entry in stream:
            entry_id = self._parse_id(entry["id"])
            if entry_id[0] >= start_id[0] and entry_id[0] <= end_id[0]:
                if entry_id[0] == start_id[0] and entry_id[1] < start_id[1]:
                    continue
                if entry_id[0] == end_id[0] and entry_id[1] > end_id[1]:
                    continue
                content = [entry["id"], [item for pair in entry["data"].items() for item in pair]]
                result.append(content)
        return result
    
    def xread(self, store_type: str, key: str, id: str) -> Union[deque, NullArray]:
        """Read stream entries with ID greater than the given ID."""
        if key not in self.stream_store:
            return NullArray()
        
        #parse the given ID
        given_id = self._parse_id(id)
        stream = self.stream_store[key]
        print("Given ID:", given_id)
        entries = deque()
        for entry in stream:
            entry_id = self._parse_id(entry["id"])
            print("Entry ID:", entry_id)
            if entry_id[0] > given_id[0] or (entry_id[0] == given_id[0] and entry_id[1] > given_id[1]):
                entries.append([entry["id"], [item for pair in entry["data"].items() for item in pair]])
        if not entries:
            return NullArray()
        result = deque([[key, entries]])
        print("XREAD result:", result)
        return result

    
    # ---- Helper Functions ----

    def _parse_stream_field_value_pairs(self, pairs: tuple):
        """Helper function to parse field-value pairs for XADD command."""
        if len(pairs) %2 != 0:
            return ErrorResponse("Field-value pairs must be even in number")
        
        data = {}
        for i in range(0, len(pairs), 2):
            field = pairs[i]
            value = pairs[i + 1]
            data[field] = value
        return data
    
    def _parse_stream_id(self, id: str, key: str) -> Union[str, ErrorResponse]:
        """Helper function to parse/validate/auto-generate stream ID for XADD command."""
        # get previous milliseconds and sequence
        prev_milliseconds, prev_sequence = 0, 0
        prev_entry = self.stream_store[key][-1] if key in self.stream_store and len(self.stream_store[key]) > 0 else None
        if prev_entry:
            prev_milliseconds, prev_sequence = map(int, prev_entry["id"].split('-'))
        # Auto-generate ID if needed
        if id == '*':
            milliseconds = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
            if milliseconds > prev_milliseconds:
                sequence = 0
            else:
                sequence = prev_sequence + 1
            return f"{milliseconds}-{sequence}" 
            
        parts = id.split('-')
        if len(parts) != 2:
            return ErrorResponse("Invalid stream ID format")
        milliseconds, sequence = parts
        milliseconds = int(milliseconds)
        
        
        # Validate milliseconds and sequence
        if sequence == '*':
            if milliseconds < prev_milliseconds:
                return ErrorResponse("The ID specified in XADD is smaller than the target stream top item")
            elif milliseconds == prev_milliseconds:
                sequence = prev_sequence + 1
            else:
                sequence = 1 if milliseconds == 0 else 0
        else:
            sequence = int(sequence)
            if milliseconds == 0 and sequence == 0:
                return ErrorResponse("The ID specified in XADD must be greater than 0-0")
            if milliseconds < prev_milliseconds or (milliseconds == prev_milliseconds and sequence <= prev_sequence):
                return ErrorResponse("The ID specified in XADD is equal or smaller than the target stream top item")
        
        return f"{milliseconds}-{sequence}"
    
    def _parse_id(self, id_str: str) -> tuple[int, int]:
        """Helper function to parse a stream ID string into (milliseconds, sequence)."""
        parts = id_str.split('-')
        milliseconds, sequence = int(parts[0]), int(parts[1]) if len(parts) > 1 else 0
        return (milliseconds, sequence)