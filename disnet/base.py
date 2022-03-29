import queue
import socket
import pickle
import threading
import os
import uuid

from pymemcache.client import base
from collections import namedtuple

from typing import Dict

from .job import Job
from .gui import Gui, Shared
import time
import gc

import logging
import shutil
# Creating and Configuring Logger

if os.path.isfile("..\\logfile.log"):
    os.remove("..\\logfile.log")

Log_Format = "%(levelname)s %(asctime)s - %(message)s"


logging.basicConfig(filename="logfile.log",
                    filemode="w",
                    format=Log_Format,
                    level=logging.INFO)

logger = logging.getLogger()

Info = namedtuple("Info", ["keys", "supported_jobs"])
Thread = namedtuple("Thread", ["target", "args"])
Data = namedtuple("Data", ["type", "args", "reassign"])

JOB = "job"
COUNTER = "counter"
ANY_JOB = "any"
print("done")


class Server:
    def __init__(self):
        # os.system("memcached -d start")
        self.clients: Dict[ANY_JOB, list] = {
            ANY_JOB: []
        }
        self.flags = {}
        self.addr_keys = {}
        self.job_mc = base.Client(('127.0.0.1', 11211))
        self.resp_mc = base.Client(('127.0.0.1', 11211))
        self.job_setter = base.Client(('127.0.0.1', 11211))
        # testing memcached
        while True:
            try:
                self.job_setter.set(COUNTER, 0)
                self.job_mc.get(COUNTER)
                self.resp_mc.get(COUNTER)
            except:
                print("error")
                time.sleep(1)
            finally:
                break
        self.sock = self._bind_socket(8090)
        self.job_index = 0
        self.jobs_queue = queue.Queue()
        self.exit_signal = threading.Event()
        self.threads: queue.Queue[threading.Thread] = queue.Queue()
        self.gui: Gui
        t = threading.Thread(target=self.tk_mainloop)
        self.threads.put(t)
        try:
            self._start_server()
        except Exception as e:
            print(e)

    def tk_mainloop(self):
        self.gui = Gui()
        self.gui.start()

    @staticmethod
    def _bind_socket(port):
        sock = socket.socket()
        sock.bind(('0.0.0.0', port))
        sock.listen()
        return sock

    def _start_server(self):
        working_threads = [
            threading.Thread(target=self._sign_new_clients),
            threading.Thread(target=self._assign_jobs),
            threading.Thread(target=self._process_data)
        ]
        for t in working_threads:
            t.start()
        print("Server is up. Waiting...\n"
              "Exit with keyboard interrupt (ctrl^C)")
        try:
            while not self.exit_signal.is_set():
                if self.threads.qsize() > 0:
                    t = self.threads.get()
                    t.start()
                    working_threads.append(t)
                time.sleep(0.2)
        except KeyboardInterrupt:  # on keyboard interrupt...
            self.exit_signal.set()
            for client_lst in self.clients.copy().values():
                for client in client_lst:
                    client.close()
            self.sock.close()
        for t in working_threads:
            t.join()

    def _sign_new_clients(self):
        while not self.exit_signal.is_set():
            try:
                client, addr = self.sock.accept()
            except:
                return
            keys, supported_jobs = pickle.loads(client.recv(2048))
            if supported_jobs[0] == "admin":
                jobs = supported_jobs[1]
                for job in jobs:
                    Shared.jobs.put(job)
                print("Server supports jobs", jobs)
                supported_jobs = []
            else:
                for job in supported_jobs:
                    if job not in self.clients:
                        self.clients[job] = []
                    self.clients[job].append(client)
            self.flags[client.fileno()] = True
            self.addr_keys[client] = keys
            print("new client", addr, supported_jobs, keys)
            self.threads.put(threading.Thread(target=self._handle_client, args=(client, addr)))

    def _assign_jobs(self):
        global logger
        while not self.exit_signal.is_set():
            if self.jobs_queue.qsize() > 0:
                job = self.jobs_queue.get()
                if job:
                    job = pickle.loads(job)
                    if job.type in self.clients or ANY_JOB in self.clients:
                        client = None
                        clients_available = self.clients[ANY_JOB].copy()
                        if job.type in self.clients:
                            clients_available += self.clients[job.type]
                        logger.info(f"{job.id} searching: {clients_available}")
                        logger.info(f"{job.id} flags are {self.flags}")
                        for client_index in range(0, len(clients_available)):
                            potential_client = clients_available[client_index]
                            if self.flags[potential_client.fileno()]:
                                client = potential_client
                                break
                        if client:
                            job.sock_fileno = client.fileno()
                            logger.info(f"{job.id} for client {job.sock_fileno}")
                            self.flags[job.sock_fileno] = False
                            self.job_setter.set(self.addr_keys[client][0], pickle.dumps(job))
                            del job
                        else:
                            job.ttl -= 1
                            if job.ttl > 0:
                                logger.info(f"{job.id} no clients, retrying... ({self.flags})")
                                self.jobs_queue.put(pickle.dumps(job))
                            else:
                                Shared.stats[1] += 1
                                logger.info(f"{job.id} dump not clients...")
                else:
                    print("none job")

    def _remove_client(self, client):
        print("removing client")
        for lst in self.clients.values():
            if client in lst:
                lst.remove(client)
        del self.flags[client.fileno()]
        del self.addr_keys[client]

    def _handle_client(self, client, addr):
        while not self.exit_signal.is_set():
            try:
                length = client.recv(4).decode()
                data = client.recv(int(length))
                if data:
                    self.jobs_queue.put(data)
                    Shared.stats[0] += 1
            except:
                if not self.exit_signal.is_set():
                    self._remove_client(client)
                return
        print("removed client {}".format(addr))

    def _process_data(self):
        while not self.exit_signal.is_set():
            self._look_for_responses()

    def _look_for_responses(self):
        keys = self.addr_keys.copy().values()
        for job, resp in keys:
            data: Data
            ret = self.resp_mc.get(resp)
            if ret:
                data, file_no = pickle.loads(ret)
                self.flags[file_no] = True
                if data and data.args[0]:
                    self.resp_mc.set(resp, b'')
                    self.resp_mc.delete(job)
                    self.resp_mc.delete(resp)
                    print(f"New Data. Type={data.type}, Args={data.args}")
                    gc.collect()


class Client:
    def __init__(self, host):
        self.host = host
        self.mc = base.Client((self.host, 11211))
        self.job_mc = base.Client((self.host, 11211))
        # testing memcached
        while True:
            try:
                self.mc.set('_key', 0)
                self.job_mc.get('_key')
            except:
                print("client is not available... retrying...")
                time.sleep(1)
            finally:
                break
        self.sock_client = socket.socket()
        self.job_key = None
        self.resp_key = str(hash(socket.gethostname()))
        self.jobs = {}
        self.lock = threading.Lock()
        self.exit_signal = threading.Event()
        self.job_queue = queue.Queue()

    def connect(self, supported_jobs=(ANY_JOB,)):
        self.sock_client.connect((self.host, 8090))
        self.job_key = str(hash(self.sock_client.getsockname()[0]))
        self.sock_client.send(pickle.dumps(
            Info((self.job_key, self.resp_key), supported_jobs)
        ))
        working_threads = []
        if len(supported_jobs) > 0:
            working_threads.append(threading.Thread(target=self._look_for_jobs))
            print("Client is up. Waiting...")
        else:
            print("Client support no jobs. Probably an admin...")

        for t in working_threads:
            t.start()

    def maintain_connection(self):
        while not self.exit_signal.is_set():
            try:
                data = self.sock_client.recv(32)
                if not data:
                    raise Exception
            except:
                self.exit_signal.set()
                print("Goodbye :)")
                return

    def _look_for_jobs(self):
        while not self.exit_signal.is_set():
            ret = self.mc.get(self.job_key)
            qsize = self.job_queue.qsize()
            if not ret and qsize == 0:
                continue
            elif qsize > 0:
                if ret:
                    self.job_queue.put(ret)
                job: Job = pickle.loads(self.job_queue.get())
            else:
                job: Job = pickle.loads(ret)

            self.mc.set(self.job_key, b'')
            if job.type in self.jobs:
                func = self.jobs[job.type]
                args = pickle.loads(self.mc.get(job.args))
                ret = func(*args)
                self.mc.set(self.resp_key, pickle.dumps((ret, job.sock_fileno)))
                self.mc.delete(job.args)
            else:
                self.mc.set(self.resp_key, pickle.dumps((None, None)))
            del job
            gc.collect()


class Admin(Client):
    def __init__(self, host):
        super().__init__(host)

    def add_job(self, job):
        args_key = str(uuid.uuid1())
        self.job_mc.set(args_key, pickle.dumps(job.args))
        job.args = args_key
        ret = pickle.dumps(job)
        st_size = str(len(ret))
        st_size = '0' * (4 - len(st_size)) + st_size
        self.sock_client.send(st_size.encode())
        self.sock_client.send(ret)
