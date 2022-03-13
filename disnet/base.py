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
import time
import gc

Info = namedtuple("Info", ["keys", "supported_jobs"])
Thread = namedtuple("Thread", ["target", "args"])
Data = namedtuple("Data", ["type", "args", "reassign"])

JOB = "job"
COUNTER = "counter"
ANY_JOB = "any"


class Server:
    def __init__(self, admins=None):
        os.system("memcached -d start")
        self.clients: Dict[ANY_JOB, list] = {
            ANY_JOB: []
        }
        self.flags = {}
        self.addr_keys = {}
        self.job_mc = base.Client(('127.0.0.1', 11211))
        self.resp_mc = base.Client(('127.0.0.1', 11211))
        self.job_mc.set(COUNTER, 0)
        self.sock = self._bind_socket(8090)
        self.lock = threading.Lock()
        self.job_index = 0
        self.jobs_queue = queue.Queue()
        self.admins = admins
        self.exit_signal = threading.Event()
        self.threads: queue.Queue[threading.Thread] = queue.Queue()
        self._start_server()

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
                    try:
                        client.close()
                    except:
                        pass
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
            for job in supported_jobs:
                if job not in self.clients:
                    self.clients[job] = []
                self.clients[job].append(client)
            self.flags[client.fileno()] = True
            self.addr_keys[client] = keys
            print("new client {}".format(addr))
            self.threads.put(threading.Thread(target=self._handle_client, args=(client, addr)))

    def _assign_jobs(self):
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
                        for client_index in range(0, len(clients_available)):
                            potential_client = clients_available[client_index]
                            if self.flags[potential_client.fileno()]:
                                client = potential_client
                                break
                        if client:
                            job.sock_fileno = client.fileno()
                            self.flags[job.sock_fileno] = False
                            self.lock.acquire()
                            self.job_mc.set(self.addr_keys[client][0], pickle.dumps(job))
                            self.lock.release()
                            del job
                        elif job.ttl == -1:
                            self.jobs_queue.put(pickle.dumps(job))
                        else:
                            job.ttl -= 1
                            if job.ttl > 0:
                                self.jobs_queue.put(pickle.dumps(job))

    def _remove_client(self, client):
        for lst in self.clients.values():
            if client in lst:
                lst.remove(client)
        del self.flags[client.fileno()]
        del self.addr_keys[client]

    def _handle_client(self, client, addr):
        while not self.exit_signal.is_set():
            try:
                data = client.recv(2048)
                if not data:
                    raise Exception
            except:
                if not self.exit_signal.is_set():
                    self._remove_client(client)
                return
        print("removed client {}".format(addr))

    def _process_data(self):
        while not self.exit_signal.is_set():
            self._look_for_jobs()
            self._look_for_responses()

    def _look_for_jobs(self):
        if self.job_index != int(self.job_mc.get(COUNTER)):
            job = self.job_mc.get(JOB + str(self.job_index))
            self.jobs_queue.put(job)
            self.job_mc.delete(JOB + str(self.job_index))
            self.job_index += 1
            gc.collect()

    def _look_for_responses(self):
        keys = self.addr_keys.copy().values()
        data: Data
        job: Job
        for job, resp in keys:
            ret = self.resp_mc.get(resp)
            if ret:
                data, job_obj, client_fileno = pickle.loads(ret)
                self.flags[client_fileno] = True
                self.resp_mc.set(resp, b'')
                self.resp_mc.delete(job)
                self.resp_mc.delete(resp)
                if data:
                    if data.reassign:
                        job_obj.type = data.reassign
                        job_obj.id = uuid.uuid1()
                        job_obj.ttl = -1
                        self.jobs_queue.put(pickle.dumps(job_obj))
                        print(f"Re assigned job [{job_obj.type}] to job [{data.reassign}]")
                    else:
                        print(f"New data type=[{data.type}], args=[{data.args}]")
                    del data
                gc.collect()


class Client:
    def __init__(self, host):
        self.host = host
        self.mc = base.Client((self.host, 11211))
        self.job_mc = base.Client((self.host, 11211))
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
                data: Data = func(*job.args)
                self.mc.set(self.resp_key, pickle.dumps((data, job, job.sock_fileno)))
            else:
                self.mc.set(self.resp_key, pickle.dumps(((None,), None, job.sock_fileno)))
            del job
            gc.collect()


class Admin(Client):
    def __init__(self, host):
        super().__init__(host)

    def add_job(self, job_or_bytes):
        if isinstance(job_or_bytes, bytes):
            while True:
                index, cas = self.job_mc.gets(COUNTER)
                index = int(index)
                if self.job_mc.cas(COUNTER, (index + 1), cas):
                    self.job_mc.set(JOB + str(index), job_or_bytes)
                    break
        else:
            self.add_job(pickle.dumps(job_or_bytes))
