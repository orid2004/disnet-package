from collections import namedtuple


class Codes:
    # Info
    KEY_EX = 100
    MEM_HOST = 101
    # Response
    APPROVAL = 200
    ERROR = 201
    DATA = 202
    END = 203


# Protocol
PACKET = namedtuple("PACKET", ["code", "content"])
KEY_EX = namedtuple("KEY_EX", ["key", "supported_jobs", "mode"])
APPROVAL = namedtuple("APPROVAL", ["host", "port", "mode"])
ERROR = namedtuple("ERROR", ["type", "details"])

# Other
Data = namedtuple("Data", ["type", "args", "reassign"])
Mem_Host = namedtuple("Mem_Host", ["host", "port", "id"])
JOB = "job"
ANY_JOB = "any"


class Modes:
    client = 1
    admin = 2
    memcached = 3
