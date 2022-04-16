from collections import namedtuple

Tuple_Info = namedtuple("Info", ["keys", "supported_jobs"])
Tuple_Data = namedtuple("Data", ["type", "args", "reassign"])
JOB = "job"
ANY_JOB = "any"
