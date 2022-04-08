class Job:
    def __init__(self, type, id, args, ttl=200):
        self.type = type
        self.id = id
        self.ttl = ttl
        self.args = args
        self.sock_fileno = 0
        self.time_stamp = 0