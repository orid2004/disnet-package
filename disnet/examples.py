from .base import Client, Data
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

    """
    Usage:
    from disnet.examples import SDClient
    client = SDClient("192.168.xx.yy")
    client.set_jobs(
        {
            "speedlimit": client.sl_predict
            "ocr": client.ocr_predict
        }
    )
    """

    def __init__(self, host):
        self.client = Client(host)
        self.host = host
        self.sl_model = speedlimit.Model()
        print("loas sl")
        self.sl_model.load_self()
        self.jobs = {
            "speedlimit": self.sl_predict,
            "ocr": self.ocr_predict
        }
        print("load ocr:")
        ocr.load_self()

    def add_jobs(self, *jobs):
        for job in jobs:
            if job in self.jobs:
                self.client.jobs[job] = self.jobs[job]

    def sl_predict(self, im) -> Data:
        tf_det = self.sl_model.get_tf_detections(im)
        det = self.sl_model.get_detections(image_np=im, tf_detections=tf_det)
        if "speedlimit" in det:
            return Data(type="speedlimit", args=(True,), reassign='ocr')

    def ocr_predict(self, im) -> Data:
        return Data(type="ocr", args=(ocr.predict(input_images=[im]),), reassign=False)

    def connect(self):
        print(f"Connecting to {self.host}...")
        self.client.connect(supported_jobs=["speedlimit"])