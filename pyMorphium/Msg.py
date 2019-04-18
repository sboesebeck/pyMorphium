import time

current_milli_time = lambda: int(round(time.time() * 1000))

class Msg:
    def __init__(self, name, value):
        self.name = name
        self.value = value
        self.lockedBy = "ALL"
        self.timestamp = current_milli_time()
        self.ttl = 30000
