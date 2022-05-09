from .client import Client
from .settings import Data
from selfdrive.model import speedlimit
from selfdrive import ocr

"""
Author: Ori David
orid2004@gmail.com
"""


class SDClient:
    """
    Name: Self-Drive Client
    This client handles self-driving jobs which are created
    by the selfdrive package. This example is used with
    CARLA simulator as published here:
    https://github.com/orid2004/Self-Driving-Car
    """

    def __init__(self, host):
        """
        Constructor for the example client
        :param host: Server host
        """
        self.client = Client(host)
        self.host = host
        self.sl_model = speedlimit.Model()
        # SDClient supports speedlimit prediction:
        self.jobs = {
            "speedlimit": self._sl_predict
        }
        self.supported_jobs = []
        self._load_required_models()

    def connect(self):
        """
        Connects to the server
        :return: None
        """
        self.client.connect(supported_jobs=self.supported_jobs)

    def add_jobs(self, *jobs):
        """
        Sets the supported jobs of the client
        :param jobs: supported jobs
        :return: None
        """
        for job in jobs:
            if job in self.jobs:
                self.client.functions[job] = self.jobs[job]
            self.supported_jobs.append(job)

    def _load_required_models(self):
        """
        Loads object-detection models that are required.
        :return:
        """
        self.sl_model.load_self()
        ocr.load_self()

    def _sl_predict(self, im) -> Data:
        """
        Functionality for `speedlimit` job
        :param im: image array
        :return: None / OCR detection of the speedlimit sign.
        """
        tf_det = self.sl_model.get_tf_detections(im)
        det = self.sl_model.get_detections(image_np=im, tf_detections=tf_det)
        if "speedlimit" in det:
            return Data(type="speedlimit", args=(ocr.predict(input_images=(im,)),), reassign=False)
