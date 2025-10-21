class Redis:
    def __init__(self) -> None:
        self.kv_store = {}
        pass

    def handle_command(self, command: str, *args: str):
        command = command.upper()
        if command == "PING":
            return self.ping()
        elif command == "ECHO" and args:
            return self.echo(args[0])
        elif command == "SET" and len(args) == 2:
            return self.set(args[0], args[1])
        elif command == "GET" and len(args) == 1:
            return self.get(args[0])
    
    # ---- Command Implementations ----
    def ping(self):
        return "PONG"
    
    def echo(self, msg: str):
        return msg
    
    def set(self, key: str, value: str):
        self.kv_store[key] = value
        return "OK"
    
    def get(self, key: str):
        return self.kv_store.get(key, None)