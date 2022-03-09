class Job:
    def __init__(self, type, id, args):
        self.type = type
        self.id = id
        self.ttl = 400
        self.args = args
        self.sock_fileno = 0


class Response(Job):
    def __init__(self, job: Job, resp):
        super().__init__(job.type, job.id, job.args)
        self.resp = resp