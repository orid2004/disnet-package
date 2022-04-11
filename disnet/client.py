import queue
import socket
import pickle
import threading
import datetime
print(datetime.datetime.now())
from pymemcache.client import base
from .job import Job
import time
import gc

JOB = "job"
COUNTER = "counter"
ANY_JOB = "any"


class Client:
    def __init__(self, host):
        self.host = host
        self.mc = base.Client((self.host, 11211))
        self.job_mc = base.Client((self.host, 11211))
        self.screens = {}
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
                args = self.mc.get(job.args)
                if args:
                    args = pickle.loads(args)
                    ret = func(*args)
                    self.mc.set(self.resp_key, pickle.dumps((ret, job.sock_fileno)))
                    print("response is in:", self.resp_key, "\n", ret)
                    self.mc.delete(job.args)
                else:
                    self.mc.set(self.resp_key, pickle.dumps((None, None)))
            else:
                self.mc.set(self.resp_key, pickle.dumps((None, None)))
            del job
            gc.collect()
