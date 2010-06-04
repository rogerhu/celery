
import threading

from eventlet import GreenPile
from eventlet.queue import LightQueue

from celery import log
from celery.utils.functional import curry
from celery.datastructures import ExceptionInfo

RUN = 0x1
CLOSE = 0x2
TERMINATE = 0x3


accept_lock = threading.Lock()


def do_work(target, args=(), kwargs={}, callback=None,
        accept_callback=None):
    accept_lock.acquire()
    try:
        accept_callback()
    finally:
        accept_lock.release()
    callback(target(*args, **kwargs))


class Waiter(threading.Thread):

    def __init__(self, pile):
        self.pile = pile
        self._state = None
        threading.Thread.__init__(self)

    def run(self):
        self._state = RUN
        pile = self.pile
        while self._state == RUN:
            pile.waiters.get().wait()



class TaskPool(object):
    _state = None

    def __init__(self, limit, logger=None, **kwargs):
        self.limit = limit
        self.logger = logger or log.get_default_logger()
        self._pool = None

    def start(self):
        self._state = RUN
        self._pool = GreenPile(self.limit)
        self._waiter = Waiter(self._pool)
        self._waiter.start()

    def stop(self):
        self._state = CLOSE
        self._pool.pool.waitall()
        self._waiter._state = TERMINATE
        self._state = TERMINATE

    def apply_async(self, target, args=None, kwargs=None, callbacks=None,
            errbacks=None, accept_callback=None, **compat):
        if self._state != RUN:
            return
        args = args or []
        kwargs = kwargs or {}
        callbacks = callbacks or []
        errbacks = errbacks or []

        on_ready = curry(self.on_ready, callbacks, errbacks)

        self.logger.debug("GreenPile: Spawn %s (args:%s kwargs:%s)" % (
            target, args, kwargs))

        self._pool.spawn(do_work, target, args, kwargs,
                                on_ready, accept_callback)
        self._pool.next()

    def on_ready(self, callbacks, errbacks, ret_value):
        """What to do when a worker task is ready and its return value has
        been collected."""

        if isinstance(ret_value, ExceptionInfo):
            if isinstance(ret_value.exception, (
                    SystemExit, KeyboardInterrupt)): # pragma: no cover
                raise ret_value.exception
            [errback(ret_value) for errback in errbacks]
        else:
            [callback(ret_value) for callback in callbacks]
