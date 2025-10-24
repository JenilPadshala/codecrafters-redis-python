from collections import deque
import asyncio
class Connection:
    """Represents a client connection to the Redis server."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer
        self.address = self.writer.get_extra_info('peername')
        self.in_multi: bool = False
        self.transaction_queue: deque = deque()

    

