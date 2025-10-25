from .redis import Redis
from .connection import Connection
from .parsing import parse_request, build_response
from collections import deque
import asyncio
import socket
BUF_SIZE = 1024

class RedisServer:
    def __init__(self, host='localhost', port=6379, role='master', replicaof=None):
        self.host = host
        self.port = port
        self.replicaof = replicaof
        self.redis_instance = Redis(self)
        self.clients = set()
        self.ip_address = socket.gethostbyname(self.host)
        self.role = role
        self.master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        self.master_repli_offset = 0
        
        # determine role
        if replicaof:
            self.role = "slave"
            self.master_host, self.master_port = replicaof.split(" ")
            self.master_port = int(self.master_port)
        else:
            self.role = "master"
            self.master_host = None
            self.master_port = None
    
    async def handle_client(self, reader, writer):
        """Handle a single client connection."""
        client = Connection(reader, writer)
        self.clients.add(client)
        try:
            while True:
                data = await client.reader.read(BUF_SIZE)
                if not data:
                    print(f"Client {client.address} disconnected")
                    break
                
                data_parsed = parse_request(data)
                if data_parsed:
                    response = await self.redis_instance.handle_command(client, data_parsed[0], *data_parsed[1:])
                    client.writer.write(build_response(response))
                else:
                    client.writer.write(build_response(None))
                
                await client.writer.drain()
        except ConnectionError:
            print(f"Client {client.address} connection reset")
        except Exception as e:
            print(f"An error occurred with {client.address}: {e}")
        finally:
            self.clients.discard(client)
            writer.close()
            await writer.wait_closed()
    
    async def start(self):
        """Start the Redis server."""
        return await asyncio.start_server(self.handle_client, self.host, self.port)
    
    async def connect_to_master(self):
        """Connect to the master server for replication."""
        if self.role != "slave":
            return
        try:
            reader, writer = await asyncio.open_connection(
                self.master_host, 
                self.master_port
            )
            print(f"Connected to master at {self.master_host}:{self.master_port}")
            await self.repl_handshake(reader, writer)
        except Exception as e:
            print(f"Failed to connect to master: {e}")

    async def repl_handshake(self, reader, writer):
        """Perform the replication handshake with the master server."""
        # Send PING command to master
        ping_command = build_response(["PING"])
        writer.write(ping_command)
        await writer.drain()
        print("Sent PING to master for replication handshake")

        # Read master's response
        response = await reader.read(BUF_SIZE)
        print(f"Received from master: {response.decode()}")