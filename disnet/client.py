import datetime
import socket
import pickle
import threading
import time
import uuid
import os
import wget
import ssl
import random
import logging

from zipfile import ZipFile
from pymemcache.client import base
from .job import Job
from .settings import *

MEMCACHED_URL = "http://static.runoob.com/download/memcached-win64-1.4.4-14.zip"

LOG_FILE = f"client_log{random.randrange(0, 1000)}.log"
Log_Format = "%(levelname)s %(asctime)s - %(message)s"


class Client:
    def __init__(self, host):
        """
        Constructor for a Client
        :param host: Server host
        """
        self.host = host
        self.mc_listener = None
        self.sock_client = self._wrap_socket(host)
        self.job_key = None
        self.functions = {}
        self.exit_signal = threading.Event()
        self.mode = Modes.client

        logging.basicConfig(filename=LOG_FILE,
                            filemode="w",
                            format=Log_Format,
                            level=logging.INFO)
        self.logger = logging.getLogger()

        self.count = 0
    
    def connect(self, supported_jobs=(ANY_JOB,)):
        while True:
            self.sock_client = self._wrap_socket(self.host)
            ret = self._connect(supported_jobs=supported_jobs)
            if ret:
                print("Connected successfully. Breaking...")
                break
            time.sleep(2)

    def _connect(self, supported_jobs=(ANY_JOB,)):
        """
        Connects to the server and sends the information required
        for the communication.
        :param supported_jobs: The offered services by the client-server.
        :return: None
        """
        print(f"Connecting to {self.host}...")
        while True:
            try:
                self.sock_client.connect((self.host, 8090))
                break
            except Exception as e:
                print(e)
                print(f"{datetime.datetime.now()} Trying to connect...")
                time.sleep(2)
        self.job_key = str(uuid.uuid1())
        self.sock_client.send(
            pickle.dumps(
                PACKET(
                    code=Codes.KEY_EX,
                    content=KEY_EX(
                        key=self.job_key,
                        supported_jobs=supported_jobs,
                        mode=self.mode
                    )
                )
            )
        )
        ret = self.sock_client.recv(1024)
        packet: PACKET = pickle.loads(ret)
        if packet.code == Codes.ERROR:
            error: ERROR = packet.content
            print(datetime.datetime.now(), "Error", error.type, "Details:", error.details)
            return 0
        elif packet.code == Codes.APPROVAL:
            approval: APPROVAL = packet.content
            if approval.mode == self.mode:
                print(f"Got an approval\nMemcached details: {approval.host}\nStarting...")
                self.mc_listener = base.Client((
                    approval.host,
                    approval.port
                ))
                self._verify_memcache()
                self._start_client(supported_jobs)
            elif approval.mode == Modes.memcached:
                self._start_memcached()
            return 1

    @staticmethod
    def _wrap_socket(hostname):
        """
        Create a secure connection
        :return: socket.socket object
        """
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        inner_sock = socket.socket()
        secure_sock = ctx.wrap_socket(inner_sock, server_hostname=hostname)
        return secure_sock

    def _start_memcached(self):
        """
        Starts a memcached server.
        :return: None
        """
        fpath = '.\\tmp.zip'
        target_path = 'c:\\Windows\\System32'
        exe = os.path.join(target_path, "memcached", "memcached.exe")
        if not os.path.isdir(os.path.join(target_path, "memcached")):
            print("Installing Memcached-Windows 64-bit...")
            wget.download(MEMCACHED_URL, fpath)
            with ZipFile(fpath, 'r') as f:
                f.extractall(target_path)
            os.remove(fpath)
            os.system(f'{exe} -d install')
        print('Running Memcache 64-bit')
        os.system(f'{exe} -d start')
        input("enter to quit>\n")
        self.sock_client.send(
            pickle.dumps(
                PACKET(
                    code=Codes.END,
                    content=None
                )
            )
        )
        raise KeyboardInterrupt

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
            except Exception as e:
                print("Error: Memcache is not available -> retrying...\n Details:", e)
                time.sleep(2)
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
            try:
                self.logger.info("waiting for ping")
                self.sock_client.recv(1)
            except Exception as e:
                print("Exception", e)
                self.exit_signal.set()
                self.connect()
                return
            self.logger.info("getting job from memcached")
            ret = self.mc_listener.get(self.job_key)
            if not ret:
                time.sleep(0.1)
                continue
            job: Job = pickle.loads(ret)
            self.mc_listener.set(self.job_key, b'')
            ret = None
            self.logger.info("running function")
            if job.type in self.functions:
                func = self.functions[job.type]
                args = self.mc_listener.get(job.args)
                if args:
                    # Process the data and get output
                    args = pickle.loads(args)
                    ret = func(*args)
            self.logger.info("pickling...")
            ret = pickle.dumps((ret, job.client_id))
            st_size = str(len(ret))
            st_size = '0' * (8 - len(st_size)) + st_size
            self.logger.info("SENDING!")
            self.sock_client.send(st_size.encode())
            self.sock_client.send(ret)
            del job

            self.count += 1
            if self.count == 50:
                open(LOG_FILE, 'w').close()
                self.count = 0


class Admin(Client):
    """
    Constructor for admin class.
    """

    def __init__(self, host):
        super().__init__(host)
        self.mode = Modes.admin
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
