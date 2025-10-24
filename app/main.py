import asyncio
from .redis import Redis
from .parsing import parse_request, build_response
from .connection import Connection
import argparse

arg_parser = argparse.ArgumentParser()

BUF_SIZE = 1024
redis_obj = Redis()

def parsed_args():
    """Parse command line arguments."""
    arg_parser.add_argument(
        "--port",
        type=int,
        default=6379,
        help="Port number for the Redis server to listen on (default: 6379)",
    )
    return arg_parser.parse_args()


async def handle_client(reader, writer):
    """
    This function is called for each new client connection.
    'reader' and 'writer' are asyncio Stream objects.
    """
    client = Connection(reader, writer)
    while True:
        try:
            data = await client.reader.read(BUF_SIZE)
            if not data:
                print(f"Client {client.address} disconnected")
                break
            data_parsed = parse_request(data)

            if data_parsed:
                response = await redis_obj.handle_command(client, data_parsed[0], *data_parsed[1:])
                client.writer.write(build_response(response))
            else:
                client.writer.write(build_response(None))
            await client.writer.drain()
        except ConnectionError:
            print(f"Client {client.address} connection reset")
            break
        except Exception as e:
            print(f"An error occurred with {client.address}: {e}")
            break

    # client_addr = writer.get_extra_info('peername')
    # print(f"Accepted connection from {client_addr}")

    # while True:
    #     try:
    #         data = await reader.read(BUF_SIZE)
    #         if not data:
    #             print(f"Client {client_addr} disconnected")
    #             break

    #         data_parsed = parse_request(data)

    #         if data_parsed:
    #             response = await redis_obj.handle_command(data_parsed[0], *data_parsed[1:])
    #             writer.write(build_response(response))
    #         else:
    #             writer.write(build_response(None))
    #         await writer.drain()
    #     except ConnectionError:
    #         print(f"Client {client_addr} connection reset")
    #         break
    #     except Exception as e:
    #         print(f"An error occurred with {client_addr}: {e}")
    #         break
    
    # print(f"Closing connection to {client_addr}")
    # writer.close()
    # await writer.wait_closed()

async def main():
    """Main entry point to start the Redis server."""
    
    args = parsed_args()
    
    server = await asyncio.start_server(handle_client, 'localhost', args.port)
    addr = server.sockets[0].getsockname()
    print(f'Server is listening on {addr}')
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nServer is shutting down.")

