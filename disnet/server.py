import datetime
import queue
import socket
import pickle
import threading
import os
import time
import gc
import logging
import ssl

from pymemcache.client import base
from typing import Dict
from .gui import Gui, Shared, Event
from .security import *
from .settings import *

LOG_FILE = "logfile.log"
Log_Format = "%(levelname)s %(asctime)s - %(message)s"
MEM_PORT = 11211


class Server:
    def __init__(self, admins, start_gui=False):
        """
        Constructor for Server.
        :param admins: allowed admins in this network.
        :param start_gui: boolean
        """
        self.clients: Dict[ANY_JOB, list] = {
            ANY_JOB: []
        }
        self.all_clients = []
        self.flags = {}
        self.mc_address_key = {}
        self.client_as_address = {}
        self.mem_host = Mem_Host(host='', port=0, id=0)
        self.mc_writer = None
        self.sock = self._bind_socket(8090)
        self.jobs_queue = queue.Queue()
        self.exit_signal = threading.Event()
        self.threads: queue.Queue[threading.Thread] = queue.Queue()
        self.admins = admins
        self.gui: Gui
        if start_gui:
            threading.Thread(target=self._tk_mainloop).start()

        if os.path.isfile(LOG_FILE):
            os.remove(LOG_FILE)

        logging.basicConfig(filename=LOG_FILE,
                            filemode="w",
                            format=Log_Format,
                            level=logging.INFO)
        self.logger = logging.getLogger()

    def start_server(self):
        """
        Starts all the threads.
        :return: None
        """
        try:
            self._start_server()
        except Exception as e:
            self.logger.info(f'Server failed to start. Details: {e}')

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
                self.logger.info("Memcache is up.")
                return
            except Exception as e:
                print(e, "Retrying...")
                self.logger.info(f"Failed to write to memcached. Details: {e}")
                time.sleep(1)

    def _set_memcached(self):
        while True:
            try:
                print("[memcached] trying to connect...")
                self.mc_writer = base.Client(self.mem_host[:2])
                break
            except Exception as e:
                print(e)
                time.sleep(2)
        self._verify_memcache()

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
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        cert_gen()
        ctx.load_cert_chain(CERT_FILE, KEY_FILE)
        inner_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        inner_sock.bind(("0.0.0.0", port))
        inner_sock.listen()
        secure_sock = ctx.wrap_socket(inner_sock, server_side=True)
        cert_clean()
        return secure_sock

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
                time.sleep(0.5)
        except KeyboardInterrupt:  # on keyboard interrupt...
            self.exit_signal.set()
            for client_lst in self.clients.copy().values():
                for client in client_lst:
                    client.close()
            self.sock.close()
            self.logger.info("Received a keyboard interrupt. Exit signal is now set.")
        self.logger.info("Joining threads.")
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
                self.logger.info(f"Exception in _sign_new_clients: {e}")
                continue
            ret = client.recv(2048)
            packet: Packet = pickle.loads(ret)
            if packet.type == "key_ex":
                key_ex: Key_Ex = packet.packet
                key, supported_jobs, mode = key_ex
                if mode == Modes.admin:
                    if self.mem_host.host == '':
                        response = Error(
                            type="memcached",
                            details="No memcached server."
                        )
                        packet_type = "error"
                    elif addr[0] not in self.admins:
                        response = Error(
                            type="forbidden",
                            details="You do not have permission to connect as admin."
                        )
                        packet_type = "error"
                    else:
                        print("Admin has connected", addr)
                        response = Approval(
                            host=self.mem_host.host,
                            port=self.mem_host.port,
                            mode=Modes.admin
                        )
                        packet_type = "approval"
                        # Insert to TREAD ABA
                        for job in supported_jobs:
                            Shared.jobs.put(job)
                        self.threads.put(threading.Thread(target=self._handle_admin, args=(client, addr)))
                    client.send(
                        pickle.dumps(
                            Packet(
                                type=packet_type,
                                packet=response
                            )
                        )
                    )
                else:
                    if not self.mem_host.host:
                        client.send(
                            pickle.dumps(
                                Packet(
                                    type="approval",
                                    packet=Approval(
                                        host="",
                                        port=MEM_PORT,
                                        mode=Modes.memcached
                                    )
                                )
                            )
                        )
                        self.mem_host = Mem_Host(host=addr[0], port=MEM_PORT, id=addr[1])
                        log = f"Memcached server on " + str(addr)
                        self._set_memcached()
                        self.threads.put(threading.Thread(target=self._handle_memcached_host, args=(client, addr)))
                    else:
                        Shared.servers.put([addr, supported_jobs])
                        for job in supported_jobs:
                            if job not in self.clients:
                                self.clients[job] = []
                            self.clients[job].append(client)
                        client.send(
                            pickle.dumps(
                                Packet(
                                    type="approval",
                                    packet=Approval(
                                        host=self.mem_host.host,
                                        port=MEM_PORT,
                                        mode=mode
                                    )
                                )
                            )
                        )
                        self.flags[addr] = True
                        self.mc_address_key[client] = key
                        log = f"New client {addr} {supported_jobs}"
                        self.threads.put(threading.Thread(target=self._handle_client, args=(client, addr)))
                    self.all_clients.append(client)
                    self.client_as_address[client] = addr
                    print(log, len(self.all_clients), "total clients")
                    self.logger.info(log)

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
                            if self.flags[self.client_as_address[potential_client]]:
                                client = potential_client
                                break
                        if client:
                            job.client_id = self.client_as_address[client]
                            Shared.events.put(
                                Event(type="inc", args=(self.client_as_address[client],))
                            )
                            self.logger.info(f"Client {job.client_id} received a job. Details: {job.id}, {job.type}")
                            self.flags[job.client_id] = False
                            self.mc_writer.set(self.mc_address_key[client], pickle.dumps(job))
                            client.send(b'\x0a')  # Ping
                            del job
                        else:
                            diff = time.time() - job.time_stamp
                            if diff < 1.5:
                                self.jobs_queue.put(job)
                            else:
                                Shared.stats[1] += 1
            else:
                time.sleep(0.2)

    def _remove_client(self, client: socket.socket):
        """
        Removes a client from this network.
        Updates all relevant dictionaries and lists.
        :param client: socket object to remove.
        :return: None
        """
        if client in self.all_clients:
            addr = self.client_as_address[client]
            print("Removing client", addr)
            self.logger.info(f"Removing client {addr}")
            self.all_clients.remove(client)
            Shared.removed_servers.put(addr)
            client.close()
            for lst in self.clients.values():
                if client in lst:
                    lst.remove(client)
            if addr in self.flags:
                del self.flags[addr]
            if client in self.mc_address_key:
                del self.mc_address_key[client]
            del self.client_as_address[client]

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
                self.logger.info(f"An exception was raised while trying to receive data from {addr}.\n"
                                 f"Details: {e}")
                if not self.exit_signal.is_set():
                    if client in self.all_clients:
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
                self.logger.info(f"An exception was raised while trying to receive data from {addr}.\n"
                                 f"Details: {e}")
                if not self.exit_signal.is_set():
                    if client in self.all_clients:
                        self._remove_client(client)
                return

    def _handle_memcached_host(self, client, addr):
        """
        Maintain the connection with the memcached host,
        and replace the host if connection is lost.
        :param client: client's socket object.
        :param addr: client's address.
        :return: None
        """

        def close_connection():
            if not self.exit_signal.is_set():
                if addr[0] == self.mem_host.host and addr[1] == self.mem_host.id:
                    self.logger.info(f"Memcached is lost. Reconnecting all clients...")
                    self.mem_host = Mem_Host(host='', port=0, id=0)
                    for c in self.all_clients.copy():
                        self._remove_client(c)

        while not self.exit_signal.is_set():
            try:
                packet: Packet = pickle.loads(client.recv(1024))
                if packet.type == "close":
                    close_connection()
                    return
            except Exception as e:
                self.logger.info(f"An exception was raised while trying to receive data from {addr}.\n"
                                 f"[Memcached host] Details: {e}")
                close_connection()
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
                log = f"Client [{file_no}] Time [{datetime.datetime.now()}]\n   Detection[{data.type} {data.args}]"
                print(log)  # Debug
                self.logger.info(log)
                Shared.stats[2] += 1
            self.logger.info(f"client {file_no} is free.")
            gc.collect()
