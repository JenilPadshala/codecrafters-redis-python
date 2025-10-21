class Redis:
    def __init__(self) -> None:
        self.kv_store = {}
        pass

    def handle_command(self, command: str, *args: str):
        command = command.upper()
        if command == "PING":
            return self.ping()
        elif command == "ECHO":
            return self.echo(*args)
    
    # ---- Command Implementations ----
    def ping(self):
        return "PONG"
    
    def echo(self, msg: str):
        return msg
    