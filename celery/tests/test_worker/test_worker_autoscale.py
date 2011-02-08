import logging

from time import time

from celery.concurrency.base import BasePool
from celery.worker import state
from celery.worker import autoscale

from celery.tests.utils import unittest, sleepdeprived

logger = logging.getLogger("celery.tests.autoscale")


class Object(object):
    pass


class MockPool(BasePool):
    shrink_raises_exception = False

    def __init__(self, *args, **kwargs):
        super(MockPool, self).__init__(*args, **kwargs)
        self._pool = Object()
        self._pool._processes = self.limit

    def grow(self, n=1):
        self._pool._processes += n

    def shrink(self, n=1):
        if self.shrink_raises_exception:
            raise KeyError("foo")
        self._pool._processes -= n

    @property
    def current(self):
        return self._pool._processes


class MockTimer(object):

    def apply_interval(self, msecs, fun, args=None, kwargs=None):

        class entry(tuple):
            cancelled = False

            def cancel(self):
                self.cancelled = True

        return entry((msecs, fun, args, kwargs))


class test_Autoscaler(unittest.TestCase):

    def setUp(self):
        self.pool = MockPool(3)

        x = autoscale.Autoscaler(MockTimer(), self.pool, 10, 3, logger=logger)
        x.start()
        self.assertTrue(x.tref)
        x.stop()
        self.assertIsNone(x.tref)

    def test_scale(self):
        x = autoscale.Autoscaler(MockTimer(), self.pool, 10, 3, logger=logger)
        x.scale()
        self.assertEqual(x.pool.current, 3)
        for i in range(20):
            state.reserved_requests.add(i)
        x.scale()
        x.scale()
        self.assertEqual(x.pool.current, 10)
        state.reserved_requests.clear()
        x.scale()
        self.assertEqual(x.pool.current, 10)
        x._last_action = time() - 10000
        x.scale()
        self.assertEqual(x.pool.current, 3)

    def test_shrink_raises_exception(self):
        x = autoscale.Autoscaler(MockTimer(), self.pool, 10, 3, logger=logger)
        x.scale_up(3)
        x._last_action = time() - 10000
        x.pool.shrink_raises_exception = True
        x.scale_down(1)
