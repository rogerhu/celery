import sys
import traceback

from time import time

from celery.worker import state


class Autoscaler(object):

    def __init__(self, timer, pool, max_concurrency, min_concurrency=0,
            keepalive=30, logger=None):
        self.timer = timer
        self.pool = pool
        self.max_concurrency = max_concurrency
        self.min_concurrency = min_concurrency
        self.keepalive = keepalive
        self.logger = logger
        self._last_action = None

        assert self.keepalive, "can't scale down too fast."

    def scale(self):
        current = min(self.qty, self.max_concurrency)
        if current > self.processes:
            self.scale_up(current - self.processes)
        elif current < self.processes:
            self.scale_down((self.processes - current) - self.min_concurrency)

    def scale_up(self, n):
        self.logger.info("Scaling up %s processes." % n)
        self._last_action = time()
        return self.pool.grow(n)

    def scale_down(self, n):
        if self._last_action or n:
            now = time()
            if now - self._last_action > self.keepalive:
                self.logger.info("Scaling down %s processes." % n)
                self._last_action = now
                try:
                    self.pool.shrink(n)
                except Exception, exc:
                    self.logger.error("Autoscaler: scale_down: %r\n%r" % (
                                        exc, traceback.format_stack()),
                                    exc_info=sys.exc_info())

    def start(self):
        self.tref = self.timer.apply_interval(1000, self.scale)

    def stop(self):
        if self.tref:
            self.tref.cancel()
            self.tref = None

    @property
    def qty(self):
        return len(state.reserved_requests)

    @property
    def processes(self):
        return self.pool._pool._processes
