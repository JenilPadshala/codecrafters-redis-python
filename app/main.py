import argparse
import asyncio
from .server import RedisServer
from collections import deque
import socket

def parsed_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--port",
        type=int,
        default=6379,
        help="Port number for the Redis server to listen on (default: 6379)",
    )
    parser.add_argument(
        "--replicaof",
        type=str,
        help="Address of the primary Redis server to replicate from (format: host port)",
    )
    return parser.parse_args()

async def main():
    args = parsed_args()
    
    if args.replicaof:
        # start replica server
        replica_server = RedisServer(host='localhost', port=args.port, role='slave', replicaof=args.replicaof)
        server = await replica_server.start()
    else:
        # start primary server
        primary_server = RedisServer(host='localhost', port=args.port, role='master')
        server = await primary_server.start()
    addr = server.sockets[0].getsockname()
    print(f'Server is listening on {addr}')

    # if replica, perform replication handshake
    if args.replicaof:
        asyncio.create_task(replica_server.connect_to_master())
        
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nServer is shutting down.")