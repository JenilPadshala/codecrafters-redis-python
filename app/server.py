from .redis import Redis
from .connection import Connection
from .parsing import parse_request, build_response
from collections import deque
import asyncio

BUF_SIZE = 1024

class RedisServer:
    def __init__(self, host='localhost', port=6379, replicaof=None):
        self.host = host
        self.port = port
        self.replicaof = replicaof
        self.redis_instance = Redis(self)
        self.clients = set()
        
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
        # Start the server on primary port
        servers = deque()
        primary_server = await asyncio.start_server(self.handle_client, self.host, self.port)
        servers.append(primary_server)

        # Optionally, start replica server
        if self.replicaof:
            master_host, master_port_str = self.replicaof.split(" ")
            master_port = int(master_port_str)
            #avoid duplicate bind
            if (master_host, master_port) != (self.host, self.port):
                replica_server = await asyncio.start_server(self.handle_client, master_host, master_port)
                servers.append(replica_server)
        
        # Print all listening addresses
        addrs = [sock.getsockname() for s in servers for sock in s.sockets]
        print(f"Servers listening on: {addrs}")

        # Run all servers concurrently
        async with asyncio.TaskGroup() as tg:
            for s in servers:
                tg.create_task(s.serve_forever())

