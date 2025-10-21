import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    print("Server is listening on localhost:6379")

    connection, client_addr = server_socket.accept()
    print(f"Client connected: {client_addr}")

    try:
        while True:
            data = connection.recv(1024)
            if not data:
                print("No data received, closing connection.")
                break

            if data == b"*1\r\n$4\r\nPING\r\n":
                connection.sendall(b"+PONG\r\n")
    finally:
        connection.close()
        print("Connection closed.")



if __name__ == "__main__":
    main()
