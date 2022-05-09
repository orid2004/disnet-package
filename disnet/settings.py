from collections import namedtuple

Packet = namedtuple("Packet", ["type", "packet"])
Key_Ex = namedtuple("Key_Ex", ["key", "supported_jobs", "mode"])
Mem_Host = namedtuple("Mem_Host", ["host", "port", "id"])
Approval = namedtuple("Approval", ["host", "port", "mode"])
Data = namedtuple("Data", ["type", "args", "reassign"])
JOB = "job"
Error = namedtuple("Error", ["type", "details"])
COUNTER = "counter"
ANY_JOB = "any"


class Modes:
    client = 1
    admin = 2
    memcached = 3
