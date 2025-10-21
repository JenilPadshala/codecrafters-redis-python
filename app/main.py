import socket  # noqa: F401
import threading

BUF_SIZE = 1024

def handle_client(client_socket: socket.socket, client_addr: tuple):
    print(f"Client connected: {client_addr}")
    try:
        while True:
            data = client_socket.recv(BUF_SIZE)
            if not data:
                print(f"No data received from {client_addr}, closing connection.")
                break
            if data == b"*1\r\n$4\r\nPING\r\n":
                client_socket.sendall(b"+PONG\r\n")
    finally:
        client_socket.close()

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    print("Server is listening on localhost:6379")

    while True:
        client_socket, client_addr = server_socket.accept()
        threading.Thread(target=handle_client, args=(client_socket, client_addr)).start()



if __name__ == "__main__":
    main()
