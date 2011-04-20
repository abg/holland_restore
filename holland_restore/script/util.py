import os
import time
import sys
import fcntl
import select
import itertools


def format_bytes(bytes, precision=2):
    """Format an integer number of bytes to a human readable string."""
    import math

    if bytes < 0:
        raise ArithmeticError("Only Positive Integers Allowed")

    if bytes != 0:
        exponent = math.floor(math.log(bytes, 1024))
    else:
        exponent = 0

    return "%.*f%s" % (
        precision,
        bytes / (1024 ** exponent),
        ['B','KB','MB','GB','TB','PB','EB','ZB','YB'][int(exponent)]
    )

from threading import Thread, Event
import threading

class ProgressMonitor(Thread):
    def __init__(self, progressbar, data):
        super(ProgressMonitor, self).__init__()
        self.progressbar = progressbar
        self.event = Event()
        self.data = data

    def stop(self):
        self.event.set()

    def run(self):
        while not self.event.isSet():
            progress, message = self.data.poll()
            self.progressbar.render(progress, message)
            self.event.wait(0.2)
