import queue
import socket
import pickle
import threading
import os
from pymemcache.client import base
from collections import namedtuple
from job import Job
import gc

Info = namedtuple("Info", ["keys", "supported_jobs"])


class Server:
    def __init__(self, admins=None):
        self.clients = {
            "any": []
        }
        self.flags = {

        }
        self.addr_keys = {

        }
        os.system("memcached -d start")
        self.job_mc = base.Client(('127.0.0.1', 11211))
        self.resp_mc = base.Client(('127.0.0.1', 11211))
        self.job_mc.set("counter", 0)
        self.sock = socket.socket()
        self.sock.bind(('0.0.0.0', 8090))
        self.sock.listen()
        self.active = True
        self.lock = threading.Lock()
        self.job_index = 0
        self.jobs_queue = queue.Queue()
        self.admins = admins
        self._start_server()

    def _start_server(self):
        threading.Thread(target=self._sign_new_clients).start()
        threading.Thread(target=self._assign_jobs).start()
        threading.Thread(target=self._process_data).start()
        print("Server is up. Waiting...")

    def _sign_new_clients(self):
        while self.active:
            client, addr = self.sock.accept()
            keys, supported_jobs = pickle.loads(client.recv(2048))
            for job in supported_jobs:
                if job not in self.clients:
                    self.clients[job] = []
                self.clients[job].append(client)
            self.flags[client.fileno()] = True
            self.addr_keys[client] = keys
            print("new client [{}]".format(addr))
            t = threading.Thread(target=self._handle_client, args=(client, addr))
            t.start()

    def _assign_jobs(self):
        while True:
            if self.jobs_queue.qsize() > 0:
                job = self.jobs_queue.get()
                if job:
                    job = pickle.loads(job)
                else:
                    continue
                if job.type in self.clients or "any" in self.clients:
                    client = None
                    clients_available = self.clients["any"].copy()
                    if job.type in self.clients:
                        clients_available += self.clients[job.type]
                    for client_index in range(0, len(clients_available)):
                        test_client = clients_available[client_index]
                        if self.flags[test_client.fileno()]:
                            client = test_client
                            break
                    if client:
                        self.flags[client.fileno()] = False
                        job.sock_fileno = client.fileno()
                        self.lock.acquire()
                        self.job_mc.set(self.addr_keys[client][0], pickle.dumps(job))
                        self.lock.release()
                        del job
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
        while self.active:
            try:
                data = client.recv(2048)
                if not data:
                    self._remove_client(client)
                    break
            except:
                self._remove_client(client)
                break
        print("removed client [{}]".format(addr))

    def _process_data(self):
        while True:
            self._look_for_jobs()
            self._look_for_responses()

    def _look_for_jobs(self):
        if self.job_index != int(self.job_mc.get("counter")):
            job = self.job_mc.get("job" + str(self.job_index))
            self.jobs_queue.put(job)
            self.job_mc.delete("job" + str(self.job_index))
            self.job_index += 1
            gc.collect()

    def _look_for_responses(self):
        keys = self.addr_keys.copy().values()
        for job, resp in keys:
            ret = self.resp_mc.get(resp)
            if ret and ret is not "None":
                ret = pickle.loads(ret)
                data = ret[:-1]
                client_fileno = ret[-1]
                self.flags[client_fileno] = True
                self.resp_mc.set(resp, b'')
                self.resp_mc.delete(job)
                self.resp_mc.delete(resp)
                print("new data", data, type(data))
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
        self.job_queue = queue.Queue()

    def connect(self, supported_jobs=("any",)):
        self.sock_client.connect((self.host, 8090))
        self.job_key = str(hash(self.sock_client.getsockname()[0]))
        self.sock_client.send(pickle.dumps(
            Info((self.job_key, self.resp_key), supported_jobs)
        ))
        if len(supported_jobs) > 0:
            threading.Thread(target=self._look_for_jobs).start()
            print("Client is up. Waiting...")
        else:
            print("Client support no jobs. Probably an admin...")

    def _look_for_jobs(self):
        while True:
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
                ret = []
                func = self.jobs[job.type]
                ret.append(func(*job.args))
                self.mc.set(self.resp_key, pickle.dumps(tuple(ret + [job.sock_fileno])))
            else:
                self.mc.set(self.resp_key, pickle.dumps("None"))
            del job


class Admin(Client):
    def __init__(self, host):
        super().__init__(host)

    def add_job(self, job_or_bytes):
        if isinstance(job_or_bytes, bytes):
            while True:
                index, cas = self.job_mc.gets("counter")
                if self.job_mc.cas("counter", (int(index) + 1), cas):
                    self.job_mc.set("job{}".format(int(index)), job_or_bytes)
                    break
        else:
            self.add_job(pickle.dumps(job_or_bytes))
