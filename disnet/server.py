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
from .settings import ANY_JOB

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
    def __init__(self, admins, start_gui=False):
        """
        Constructor for Server.
        :param admins: allowed admins in this network.
        :param start_gui: boolean
        """
        print("Warning! Please make sure memcache is running on this computer...")
        self.clients: Dict[ANY_JOB, list] = {
            ANY_JOB: []
        }
        self.flags = {}
        self.mc_address_keys = {}
        self.client_as_address = {}
        self.mc_writer = base.Client(('127.0.0.1', 11211))
        self._verify_memcache()
        self.sock = self._bind_socket(8090)
        self.jobs_queue = queue.Queue()
        self.exit_signal = threading.Event()
        self.threads: queue.Queue[threading.Thread] = queue.Queue()
        self.admins = admins
        self.gui: Gui
        if start_gui:
            threading.Thread(target=self._tk_mainloop).start()

    def start_server(self):
        """
        Starts all the threads.
        :return: None
        """
        try:
            self._start_server()
        except Exception as e:
            logger.info(f'Server failed to start. Details: {e}')

    def _verify_memcache(self):
        """
        Verifies memcache as the client depends on it.
        Memcache-windows-64bit must be started on the server side.
        :return: None
        """
        while True:
            try:
                self.mc_writer.set('_tmp', 0)
                self.mc_writer.get('_tmp')
            except Exception as e:
                print(e, "Retrying...")
                logger.info(f"Failed to write to memcache. Details: {e}")
                time.sleep(1)
            finally:
                break

    def _tk_mainloop(self):
        """
        Starts the tkinter gui.
        Note: It doesn't work yet :f
        :return: None
        """
        self.gui = Gui()
        self.gui.start()

    @staticmethod
    def _bind_socket(port):
        """
        Binds the socket locally
        :return: socket object
        """
        sock = socket.socket()
        sock.bind(('0.0.0.0', port))
        sock.listen()
        return sock

    def _start_server(self):
        """
        Private function that starts all the threads.
        It defines the exit method which is by a keyboard interrupt.
        :return: None
        """
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
            logger.info("Received a keyboard interrupt. Exit signal is now set.")
        logger.info("Joining threads.")
        for t in working_threads:
            t.join()

    def _sign_new_clients(self):
        """
        Waits for new clients and process their data.
        Each client is saved according to the jobs it offers.
        :return: None
        """
        while not self.exit_signal.is_set():
            try:
                client, addr = self.sock.accept()
            except Exception as e:
                logger.info(f"Exception in _sign_new_clients: {e}")
                continue
            keys, supported_jobs = pickle.loads(client.recv(2048))
            if supported_jobs[0] == "admin":
                # Handle admin socket
                for job in supported_jobs[1]:
                    Shared.jobs.put(job)
                supported_jobs = []
                self.threads.put(threading.Thread(target=self._handle_admin, args=(client, addr)))
            else:
                # Handle client socket
                Shared.servers.put([addr, supported_jobs])
                for job in supported_jobs:
                    if job not in self.clients:
                        self.clients[job] = []
                    self.clients[job].append(client)
                self.threads.put(threading.Thread(target=self._handle_client, args=(client, addr)))
            self.flags[client.fileno()] = True
            self.mc_address_keys[client] = keys
            self.client_as_address[client] = addr
            log = f"New client {addr} {supported_jobs}"
            print(log)
            logger.info(log)

    def _assign_jobs(self):
        """
        Assigns jobs to free clients.
        This thread manage time-stamps, as jobs are saved in
        the queue for only 1500 ms. This allows fast and relevant
        performance while taking the risk of lost jobs.
        :return: None
        """
        while not self.exit_signal.is_set():
            if self.jobs_queue.qsize() > 0:
                job = self.jobs_queue.get()
                if job:
                    if job.time_stamp == 0:
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
                                Event(type="inc", args=(self.client_as_address[client],))
                            )
                            logger.info(f"Client {job.sock_fileno} received a job. Details: {job.id}, {job.type}")
                            self.flags[job.sock_fileno] = False
                            self.mc_writer.set(self.mc_address_keys[client][0], pickle.dumps(job))
                            del job
                        else:
                            diff = time.time() - job.time_stamp
                            if diff < 1.5:
                                self.jobs_queue.put(job)
                            else:
                                Shared.stats[1] += 1
            else:
                time.sleep(0.2)

    def _remove_client(self, client):
        """
        Removes a client from this network.
        Updates all relevant dictionaries and lists.
        :param client: socket object to remove.
        :return: None
        """
        logger.info(f"Removing client {self.client_as_address[client]}")
        for lst in self.clients.values():
            if client in lst:
                lst.remove(client)
        del self.flags[client.fileno()]
        del self.mc_address_keys[client]

    def _handle_admin(self, client, addr):
        """
        Waits for data. Incoming data from an admin should be pickled jobs.
        Otherwise, the server would fail.
        :param client: admin's socket object.
        :param addr: admin's address.
        :return: None
        """
        while not self.exit_signal.is_set():
            try:
                data_length = client.recv(8).decode()
                data = client.recv(int(data_length))
                if data:
                    for job in pickle.loads(data):
                        self.jobs_queue.put(job)
                    Shared.stats[0] += 1
            except Exception as e:
                logger.info(f"An exception was raised while trying to receive data from {addr}.\n"
                            f"Details: {e}")
                if not self.exit_signal.is_set():
                    self._remove_client(client)
                return

    def _handle_client(self, client, addr):
        """
        Waits for data. Data from a client should be pickled Data objects.
        Otherwise, there's no way to forward the detections back to the admin.
        :param client: client's socket object.
        :param addr: client's address.
        :return: None
        """
        while not self.exit_signal.is_set():
            try:
                data_length = client.recv(8).decode()
                data = client.recv(int(data_length))
                self._free_client(data)
            except Exception as e:
                logger.info(f"An exception was raised while trying to receive data from {addr}.\n"
                            f"Details: {e}")
                if not self.exit_signal.is_set():
                    self._remove_client(client)
                return

    def _free_client(self, ret):
        """
        Change a client's status back to available after getting a response.
        The response consists of the data, and an identifier of the client.
        :param ret: Response
        :return: None
        """
        if ret:
            data, file_no = pickle.loads(ret)
            self.flags[file_no] = True
            if data and data.args[0]:
                log = f"Client {file_no} got a detection. Details: {data.type}, {data.args}"
                print(log)  # Debug
                logger.info(log)
                Shared.stats[2] += 1
            logger.info(f"client {file_no} is free.")
            gc.collect()
