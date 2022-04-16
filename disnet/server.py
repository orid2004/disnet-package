import queue
import socket
import pickle
import threading
import os
import time
import gc
import logging

from pymemcache.client import base
from typing import Dict
from .gui import Gui, Shared, Event
from .settings import ANY_JOB, COUNTER

LOG_FILE = "logfile.log"

if os.path.isfile(LOG_FILE):
    os.remove(LOG_FILE)

Log_Format = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(filename=LOG_FILE,
                    filemode="w",
                    format=Log_Format,
                    level=logging.INFO)
logger = logging.getLogger()


class Server:
    def __init__(self, admins):
        # os.system("memcached -d start")
        self.clients: Dict[ANY_JOB, list] = {
            ANY_JOB: []
        }
        self.flags = {}
        self.addr_keys = {}
        self.addresses = {}
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
        self.admins = admins
        self.gui: Gui
        # threading.Thread(target=self._tk_mainloop).start()

    def start_server(self):
        try:
            self._start_server()
        except Exception as e:
            print(e)

    def _tk_mainloop(self):
        self.gui = Gui()
        self.gui.start()
        pass

    @staticmethod
    def _bind_socket(port):
        sock = socket.socket()
        sock.bind(('0.0.0.0', port))
        sock.listen()
        return sock

    def _start_server(self):
        working_threads = [
            self._sign_new_clients,
            self._assign_jobs,
            self._tk_mainloop
        ]
        for t in working_threads:
            threading.Thread(target=t).start()
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
                self.threads.put(threading.Thread(target=self._handle_admin, args=(client, addr)))
            else:
                Shared.servers.put([addr, supported_jobs])
                for job in supported_jobs:
                    if job not in self.clients:
                        self.clients[job] = []
                    self.clients[job].append(client)
                self.threads.put(threading.Thread(target=self._handle_client, args=(client, addr)))
            self.flags[client.fileno()] = True
            self.addr_keys[client] = keys
            self.addresses[client] = addr
            print("new client", addr, supported_jobs, keys)

    def _assign_jobs(self):
        global logger
        while not self.exit_signal.is_set():
            if self.jobs_queue.qsize() > 0:
                job = self.jobs_queue.get()
                if job:
                    if job.time_stamp == 0:
                        logger.info(f"{job.id} New in queue. {job.time_stamp}")
                        job.time_stamp = time.time()
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
                            Shared.events.put(
                                Event(type="inc", args=(self.addresses[client],))
                            )
                            logger.info(f"{job.id} Job for client {job.sock_fileno}")
                            self.flags[job.sock_fileno] = False
                            self.job_setter.set(self.addr_keys[client][0], pickle.dumps(job))
                            del job
                        else:
                            diff = time.time() - job.time_stamp
                            if diff < 1.5:
                                self.jobs_queue.put(job)
                            else:
                                Shared.stats[1] += 1
                                logger.info(f"{job.id} Lost as no clients are available")
                else:
                    print("none job")
            else:
                time.sleep(0.2)

    def _remove_client(self, client):
        print("removing client")
        for lst in self.clients.values():
            if client in lst:
                lst.remove(client)
        del self.flags[client.fileno()]
        del self.addr_keys[client]

    def _handle_admin(self, client, addr):
        print('handle admin here')
        while not self.exit_signal.is_set():
            try:
                length = client.recv(8).decode()
                data = client.recv(int(length))
                if data:
                    for job in pickle.loads(data):
                        self.jobs_queue.put(job)
                    Shared.stats[0] += 1
            except:
                if not self.exit_signal.is_set():
                    self._remove_client(client)
                return
        print("removed admin {}".format(addr))

    def _handle_client(self, client, addr):
        print("handle client here")
        while not self.exit_signal.is_set():
            try:
                length = client.recv(8).decode()
                data = client.recv(int(length))
                self.free_client(data)
            except:
                if not self.exit_signal.is_set():
                    self._remove_client(client)
                return
        print("removed client {}".format(addr))

    def free_client(self, ret):
        if ret:
            data, file_no = pickle.loads(ret)
            self.flags[file_no] = True
            if data and data.args[0]:
                print(f"DATA | Type={data.type}, Args={data.args}")
                logger.info(f"client {file_no} has a detection! {data.type, data.args}")
                Shared.stats[2] += 1
            logger.info(f"client {file_no} is free and waiting")
            gc.collect()

    """

    def _look_for_responses(self):
        keys = self.addr_keys.copy().values()
        if len(keys) == 0:
            print("ERROR ERROR")
        for job, resp in keys:
            data: Data
            ret = self.resp_mc.get(resp)
            if ret:
                data, file_no = pickle.loads(ret)
                self.flags[file_no] = True

                if data and data.args[0]:
                    print(f"New Data. Type={data.type}, Args={data.args}")
                    logger.info(f"client {file_no} has a detection! {data.type, data.args}")
                    Shared.stats[2] += 1
                logger.info(f"client {file_no} is free and waiting")
                self.resp_mc.set(resp, b'')
                self.resp_mc.delete(job)
                self.resp_mc.delete(resp)
                gc.collect()
    """
