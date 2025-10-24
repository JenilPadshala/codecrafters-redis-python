# import asyncio
# from .redis import Redis
# from .parsing import parse_request, build_response
# from .connection import Connection
# import argparse

# arg_parser = argparse.ArgumentParser()

# BUF_SIZE = 1024
# redis_obj = Redis()

# def parsed_args():
#     """Parse command line arguments."""
#     arg_parser.add_argument(
#         "--port",
#         type=int,
#         default=6379,
#         help="Port number for the Redis server to listen on (default: 6379)",
#     )
#     arg_parser.add_argument(
#         "--replicaof",
#         type=str,
#         help="Address of the primary Redis server to replicate from (format: host port)",
#         required=False,
#     )
#     return arg_parser.parse_args()


# async def handle_client(reader, writer):
#     """
#     This function is called for each new client connection.
#     'reader' and 'writer' are asyncio Stream objects.
#     """
#     client = Connection(reader, writer)
#     while True:
#         try:
#             data = await client.reader.read(BUF_SIZE)
#             if not data:
#                 print(f"Client {client.address} disconnected")
#                 break
#             data_parsed = parse_request(data)

#             if data_parsed:
#                 response = await redis_obj.handle_command(client, data_parsed[0], *data_parsed[1:])
#                 client.writer.write(build_response(response))
#             else:
#                 client.writer.write(build_response(None))
#             await client.writer.drain()
#         except ConnectionError:
#             print(f"Client {client.address} connection reset")
#             break
#         except Exception as e:
#             print(f"An error occurred with {client.address}: {e}")
#             break

# async def main():
#     """Main entry point to start the Redis server."""
    
#     args = parsed_args()
#     is_replica = args.replicaof is not None
#     if is_replica:
#         role = "slave"
#         master_host, master_port = args.replicaof.split(" ")

#     server = await asyncio.start_server(handle_client, 'localhost', args.port)
#     addr = server.sockets[0].getsockname()
#     print(f'Server is listening on {addr}')
#     async with server:
#         await server.serve_forever()


# if __name__ == "__main__":
#     try:
#         asyncio.run(main())
#     except KeyboardInterrupt:
#         print("\nServer is shutting down.")

import argparse
import asyncio
from .server import RedisServer

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
    server = RedisServer(port=args.port, replicaof=args.replicaof)
    await server.start()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nServer is shutting down.")