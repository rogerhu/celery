from celery.worker.state import SOFTWARE_INFO
from celery.utils import timer2


class Heart(object):

    def __init__(self, timer, eventer, interval=None):
        self.timer = timer
        self.eventer = eventer
        self.interval = interval or 30
        self.tref = None

    def start(self):
        dispatch = self.eventer.send
        dispatch("worker-online", **SOFTWARE_INFO)
        #self.tref = self.timer.apply_interval(self.interval * 10000.0,
        #        dispatch, ("worker-heartbeat", ), SOFTWARE_INFO)

    def stop(self):
        if self.tref is not None:
            self.tref.cancel()
            self.tref = None
        self.eventer.send("worker-offline", **SOFTWARE_INFO)
