# import socket  # noqa: F401
# import threading

# BUF_SIZE = 1024

# def handle_client(client_socket: socket.socket, client_addr: tuple):
#     print(f"Client connected: {client_addr}")
#     try:
#         while True:
#             data = client_socket.recv(BUF_SIZE)
#             if not data:
#                 print(f"No data received from {client_addr}, closing connection.")
#                 break
#             if data == b"*1\r\n$4\r\nPING\r\n":
#                 client_socket.sendall(b"+PONG\r\n")
#     finally:
#         client_socket.close()

# def main():
#     # You can use print statements as follows for debugging, they'll be visible when running tests.
#     print("Logs from your program will appear here!")

#     server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
#     print("Server is listening on localhost:6379")

#     while True:
#         client_socket, client_addr = server_socket.accept()
#         threading.Thread(target=handle_client, args=(client_socket, client_addr)).start()



# if __name__ == "__main__":
#     main()


import asyncio
import socket

BUFF_SIZE = 1024

async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """
    This coroutine is run for each client that connects to the server.
    """
    client_addr = writer.get_extra_info('peername')
    print(f"Client connected: {client_addr}")
    try:
        while True:
            data = await reader.read(BUFF_SIZE)
            if not data:
                print(f"No data received from {client_addr}, closing connection.")
                break
            if data == b"*1\r\n$4\r\nPING\r\n":
                writer.write(b"+PONG\r\n")
                await writer.drain()
    except ConnectionResetError:
        print(f"Connection reset by {client_addr}")
    except Exception as e:
        print(f"An error occurred with {client_addr}: {e}")
    finally:
        writer.close()
        await writer.wait_closed()

async def main():
    print("Logs from your program will appear here!")

    server_socket = await asyncio.start_server(handle_client, "localhost", 6379)
    print("Server is listening on localhost:6379")
    async with server_socket:
        await server_socket.serve_forever()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Server shutting down...")