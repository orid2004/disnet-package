import queue
import time
import random
import tkinter
import tkinter as tk
from datetime import datetime
# from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
# import plot
import threading
import warnings
import psutil
from collections import namedtuple

warnings.filterwarnings("ignore")

Event = namedtuple("Event", ["type", "args"])


class Shared:
    servers = queue.Queue()
    removed_servers = queue.Queue()
    jobs = queue.Queue()
    events = queue.Queue()
    data = []
    stats = [0, 0, 0]


class Statistic:
    y = 320

    def __init__(self, root, name, value, color):
        y = Statistic.y
        tk.Label(root, text=name, font='Montserrat 12 bold', bg='#0a2866', fg='#FFFFFF').place(x=25, y=y)
        tk.Canvas(root, width=160, height=20, bg="#c7d5e0", bd=0, highlightthickness=0).place(x=125, y=y + 5)
        tk.Canvas(root, width=int(1.6 * value), height=20, bg=color, bd=0, highlightthickness=0).place(x=125, y=y + 5)
        self.value_label = tk.Label(root, text=f'{value}', font='Montserrat 12 bold', bg='#0a2866', fg='#FFFFFF')
        self.value_label.place(x=300, y=y)
        Statistic.y += 30

    def config(self, value=None):
        self.value_label.config(text=f'{value}')


class Gui:
    def __init__(self):
        """

        :rtype: object
        """
        # Window config
        root = tk.Tk()
        root.resizable(False, False)
        root.geometry('1305x780+0+5')
        root.title("orid2004's Graphic User Interface")
        root.configure(bg='#c7d5e0')

        def on_closing():
            print("event false")
            Shared.event = False
            root.destroy()

        root.protocol("WM_DELETE_WINDOW", on_closing)

        # Properties
        self.root = root
        self.max_servers_in_page = 9
        self.current_page = 0
        self.num_pages = 0
        self.servers = []
        self.server_labels = []
        self.act_base_y = 300
        self.server_current_y = self.act_base_y + 20
        self.jobs = {}
        self.job_labels = {}
        self.label_by_host = {}
        self.data = Shared.data
        self.stats = Shared.stats

        # Top Canvas
        top_bg = tk.Canvas(root, width=1305, height=60, bg='#4274fd', highlightthickness=0)
        top_bg.place(x=0, y=0)
        tk.Label(top_bg, text='Dashboard', font='Montserrat 25', bg='#4274fd', fg='white').place(x=15, y=5)
        tk.Label(
            top_bg, text=datetime.now().strftime('%A, %d %B %Y'), font='Montserrat 20', bg='#4274fd', fg='white'
        ).place(x=930, y=10)

        # Plot
        # self.draw_plot()

        # Jobs Canvas
        jc_base_y = 500
        self.jc_base_y = jc_base_y
        tk.Canvas(root, width=350, height=270, bg='#0a2866', highlightthickness=0).place(x=20, y=jc_base_y)
        tk.Canvas(root, width=350, height=30, bg='#4274fd', highlightthickness=0).place(x=20, y=jc_base_y - 20)
        tk.Label(root, text='Jobs', font='Montserrat 12 bold', bg='#4274fd',
                 fg='#FFFFFF').place(x=25, y=jc_base_y - 20)

        # Admin Section
        as_base_y = 300
        tk.Canvas(root, width=285, height=470, bg='#0a2866', highlightthickness=0).place(x=1000, y=as_base_y)
        tk.Canvas(root, width=285, height=30, bg='#4274fd', highlightthickness=0).place(x=1000, y=as_base_y - 20)
        tk.Label(root, text='Admin', font='Montserrat 12 bold', bg='#4274fd', fg='#FFFFFF').place(x=1005,
                                                                                                  y=as_base_y - 20)

        # Statistics
        stat_base_y = 300
        tk.Canvas(root, width=350, height=160, bg='#0a2866', highlightthickness=0).place(x=20, y=stat_base_y)
        tk.Canvas(root, width=350, height=30, bg='#4274fd', highlightthickness=0).place(x=20, y=stat_base_y - 20)
        tk.Label(root, text='Statistics', font='Montserrat 12 bold', bg='#4274fd',
                 fg='#FFFFFF').place(x=25, y=stat_base_y - 20)
        self.all_label = Statistic(root=root, name="All Jobs", color='#f0f73c', value=100)
        # label
        self.lost_label = Statistic(root=root, name="Lost Jobs", color='#fa1e3c', value=0)
        # label
        self.detections_label = Statistic(root=root, name="Detections", color='#27bdea', value=0)
        # label
        self.mem_usage = Statistic(root=root, name="Memory", color='#7b1e7a', value=psutil.virtual_memory()[2])

        # Active Servers section
        self.act_canvas = tk.Canvas(root, width=590, height=470, bg='#0a2866', highlightthickness=0)
        self.act_canvas.place(x=390, y=self.act_base_y)
        tk.Canvas(root, width=590, height=30, bg='#4274fd', highlightthickness=0).place(x=390, y=self.act_base_y - 20)
        tk.Canvas(root, width=120, height=460, bg='#171a21', highlightthickness=0).place(x=860, y=self.act_base_y + 10)
        tk.Label(root, text='Active Server', font='Montserrat 12 bold', bg='#4274fd',
                 fg='#FFFFFF').place(x=395, y=self.act_base_y - 20)

        # Start \ Next-Page button
        def _next_page(_):
            self.current_page += 1
            if self.current_page == self.num_pages:
                self.current_page = 0
            self.config_next_btn()
            self.place_servers()

        self.next_btn = tk.Label(root, text='NEXT', fg="white", font="bold", bg='#4274fd', relief='groove')
        self.next_btn.place(x=800, y=self.act_base_y - 20, width=180, height=30)
        self.next_btn.bind('<Button-1>', _next_page)
        self.config_next_btn()

    """
    def draw_plot(self):
        fig, ax = plot.get_data_plot(self.data, 100)
        canvas = FigureCanvasTkAgg(fig, master=self.root)  # A tk.DrawingArea.
        canvas.draw()
        canvas.get_tk_widget().place(x=20, y=90)
    """

    def update_stats(self):
        if self.stats[0] != 0:
            self.lost_label.config(value=self.stats[1])
            # label update
            self.detections_label.config(value=self.stats[2])
            # label update

    def update_job_status(self):
        print(self.jobs)
        for label in self.job_labels:
            self.job_labels[label].destroy()
            del label
        current_y = self.jc_base_y + 20
        for job in self.jobs:
            tk.Label(self.root, text=job, font="Montserrat 14 bold", bg='#0a2866', fg='#FFFFFF').place(x=25,
                                                                                                       y=current_y)
            is_supported = self.jobs[job]
            #text, fg = ("Support", "green") if is_supported else ("No-Support", "red")
            #label = tk.Label(self.root, text=f"[{text}]", font='Montserrat 14 bold', bg='#0a2866', fg=fg)
            #label.place(x=200, y=current_y)
            #self.job_labels[job] = label
            current_y += 30

    def add_job(self, job):
        self.jobs[job] = []
        self.update_job_status()

    def config_next_btn(self):
        self.next_btn.config(text='Next  [Page {} / {}]'.format(self.current_page + 1, self.num_pages))

    def destroy_server_labels(self):
        for label in self.server_labels:
            label.destroy()
        self.server_current_y = self.act_base_y + 20

    def add_server(self, server):
        self.servers.append(server)
        self.update_server_list()

    def remove_server(self, server):
        self.servers.remove(server)
        self.update_server_list()

    def update_server_list(self):
        self.num_pages = len(self.servers) // (self.max_servers_in_page + 1) + 1
        self.config_next_btn()
        self.place_servers()

    def place_servers(self):
        self.destroy_server_labels()
        base_y = self.act_base_y + 20
        start = self.max_servers_in_page * self.current_page
        end = start + self.max_servers_in_page
        if end > len(self.servers):
            end = len(self.servers)
        for server in self.servers[start:end]:
            label1 = tk.Label(self.root, text=f"({server})", font='Montserrat 16', bg='#0a2866', fg='#FFFFFF')
            label1.place(x=400, y=base_y)
            label2 = tk.Label(self.root, text="0", font='Montserrat 16', bg='#171a21', fg='#FFFFFF')
            label2.place(x=880, y=base_y)
            self.label_by_host[server] = label2
            self.server_labels.append(label1)
            self.server_labels.append(label2)
            base_y += 50

    def inc_count(self, host):
        label = self.label_by_host[host]
        count = int(label["text"])
        label.config(text=str(count + 1))

    def start(self):
        try:
            self._loop()
        except tkinter.TclError:
            exit(0)

    def _loop(self):
        while True:
            self.root.update()
            while Shared.servers.qsize() > 0:
                if Shared.jobs.qsize() > 0:
                    self.add_job(Shared.jobs.get())
                server, jobs = Shared.servers.get()
                self.add_server(server)
                for job in jobs:
                    job = job.lower()
                    if job in self.jobs:
                        self.jobs[job].append(server)
                self.update_job_status()
            while Shared.removed_servers.qsize() > 0:
                server = Shared.removed_servers.get()
                if server in self.servers:
                    print("got rm")
                    print("start rm func")
                    self.remove_server(server)
                    print("done remove done update")
                else:
                    print("bad error", self.servers)
            while Shared.jobs.qsize() > 0:
                self.add_job(Shared.jobs.get())
            while Shared.events.qsize() > 0:
                event = Shared.events.get()
                if event.type == "inc":
                    self.inc_count(event.args[0])
            self.mem_usage.config(value=psutil.virtual_memory()[2])
            self.stats = Shared.stats
            self.update_stats()
            time.sleep(0.25)
