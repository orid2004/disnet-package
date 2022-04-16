import socket
import pickle
import threading
import time
import uuid

from pymemcache.client import base
from .job import Job
from .settings import Tuple_Info as Info, ANY_JOB


class Client:
    def __init__(self, host):
        """
        Constructor for a Client
        :param host: Server host
        """
        self.host = host
        self.mc_listener = base.Client((self.host, 11211))
        self._verify_memcache()
        self.sock_client = socket.socket()
        self.job_key = None
        self.functions = {}
        self.exit_signal = threading.Event()

    def connect(self, supported_jobs=(ANY_JOB,)):
        """
        Connects to the server and sends the information required
        for the communication.
        :param supported_jobs: The offered services by the client-server.
        :return: None
        """
        print(f"Connecting to {self.host}...")
        self.sock_client.connect((self.host, 8090))
        self.job_key = str(uuid.uuid1())
        self.sock_client.send(pickle.dumps(
            Info((self.job_key, None), supported_jobs)
        ))
        self._start_client(supported_jobs)

    def _start_client(self, supported_jobs):
        """
        Starts all threads
        :return: None
        """
        working_threads = []
        if len(supported_jobs) > 0:
            working_threads.append(threading.Thread(target=self._look_for_jobs))
            print("Client is up. Waiting for jobs...")

        for t in working_threads:
            t.start()

    def _verify_memcache(self):
        """
        Verifies memcache as the client depends on it.
        Memcache-windows-64bit must be started on the server side.
        :return: None
        """
        while True:
            try:
                self.mc_listener.set('_tmp', 0)
            except:
                print("Error: Memcache is not available. retrying...")
                time.sleep(1)
            finally:
                return

    def _look_for_jobs(self):
        """
        This is the main thread. It waits for jobs
        in memcache and read them. Data is processed to
        the matching function in `` dictionary.
        :return: Sends output to the server.
        """
        while not self.exit_signal.is_set():
            ret = self.mc_listener.get(self.job_key)
            if not ret:
                time.sleep(0.1)
                continue
            job: Job = pickle.loads(ret)
            self.mc_listener.set(self.job_key, b'')
            ret = None
            if job.type in self.functions:
                func = self.functions[job.type]
                args = self.mc_listener.get(job.args)
                if args:
                    # Process the data and get output
                    args = pickle.loads(args)
                    ret = func(*args)
            ret = pickle.dumps((ret, job.sock_fileno))
            st_size = str(len(ret))
            st_size = '0' * (8 - len(st_size)) + st_size
            self.sock_client.send(st_size.encode())
            self.sock_client.send(ret)
            del job

class Admin(Client):
    """
    Constructor for admin class.
    """
    def __init__(self, host):
        super()._init_(host)
        self.mc_writer = base.Client((self.host, 11211))

    def put_jobs(self, jobs):
        """
        Sends a packet consists of jobs to the server.
        Length must be sent as an 8 digits string.
        """
        data = pickle.dumps(jobs)
        st_size = str(len(data))
        st_size = '0' * (8 - len(st_size)) + st_size
        self.sock_client.send(st_size.encode())
        self.sock_client.send(data)
