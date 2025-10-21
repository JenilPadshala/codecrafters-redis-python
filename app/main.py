import socket
import selectors

BUF_SIZE = 1024

selector = selectors.DefaultSelector()

def handle_client_data(client_socket):
    """Callback for when a client socket has data to read."""
    try:
        data = client_socket.recv(BUF_SIZE)
        if not data:
            print("Client disconnected")
            selector.unregister(client_socket)
            client_socket.close()
            return
        if data == b"*1\r\n$4\r\nPING\r\n":
            client_socket.sendall(b"+PONG\r\n")
        
        if data.startswith(b"*2\r\n$4\r\nECHO\r\n"):
            parts = data.split(b"\r\n")
            msg = parts[-2]
            client_socket.sendall(b"$3\r\n"+str(msg).encode()+b"\r\n")
    except ConnectionError:
        selector.unregister(client_socket)
        client_socket.close()


def accept_connection(server_socket):
    """Callback for when the server socket has a new connection."""
    client_socket, client_addr = server_socket.accept()
    print(f"Accepted connection from {client_addr}")
    client_socket.setblocking(False)
    selector.register(client_socket, selectors.EVENT_READ, data=handle_client_data)    


def main():
    print("Logs from your program will appear here.")

    server_socket = socket.create_server(('localhost', 6379), reuse_port=True)
    server_socket.setblocking(False)

    selector.register(server_socket, selectors.EVENT_READ, data=accept_connection)
    print("Server is listening on localhost:6379")

    try:
        # The EVENT LOOP
        while True:
            events = selector.select() # Blocking call, waits for events

            # Dispatch: loop over all ready sockets
            for key, mask in events:
                callback = key.data
                callback(key.fileobj)
    except KeyboardInterrupt:
        print("Server is shutting down.")
    finally:
        selector.close()
        server_socket.close()

if __name__ == "__main__":
    main()