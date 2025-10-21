from datetime import datetime, timedelta, timezone
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
        elif command == "SET" and len(args) >= 2:
            return self.set(*args)
        elif command == "GET" and len(args) == 1:
            return self.get(args[0])
    
    # ---- Command Implementations ----
    def ping(self):
        return "PONG"
    
    def echo(self, msg: str):
        return msg
    
    def set(self, key: str, value: str, opt, expire_time):
        if opt.upper() == "EX":
            expire_time = int(expire_time) * 1000  # convert to milliseconds
        elif opt.upper() == "PX":
            expire_time = int(expire_time)
        if expire_time:
            expire_at = datetime.now() + timedelta(milliseconds=expire_time)
        else:
            expire_at = datetime.fromtimestamp(0, tz=timezone.utc)  # no expiration
        self.kv_store[key] = {"value": value, "expire_at": expire_at}
        return "OK"
    
    def get(self, key: str):
        if key in self.kv_store:
            record = self.kv_store[key]
            if record["expire_at"] > datetime.now() or record["expire_at"].timestamp() == 0;
                return record["value"]
            else:
                del self.kv_store[key]
                return None
        return None