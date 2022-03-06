class Job:
    def __init__(self, type, id, args):
        self.type = type
        self.id = id
        self.ttl = 400
        self.args = args
        self.sock_fileno = 0
