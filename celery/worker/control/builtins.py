from datetime import datetime

from celery import conf
from celery import log
from celery.backends import default_backend
from celery.registry import tasks
from celery.utils import timeutils
from celery.worker import state
from celery.worker.state import revoked
from celery.worker.control.registry import Panel

TASK_INFO_FIELDS = ("exchange", "routing_key", "rate_limit")


def revoke(state, task_id, task_name=None, **kwargs):
    """Revoke task by task id."""
    revoked.add(task_id)
    backend = default_backend
    if task_name: # Use custom task backend (if any)
        try:
            backend = tasks[task_name].backend
        except KeyError:
            pass
    backend.mark_as_revoked(task_id)
    state.logger.warn("Task %s revoked" % (task_id, ))
    return {"ok": "task %s revoked" % (task_id, )}


def enable_events(state):
    dispatcher = state.listener.event_dispatcher
    if not dispatcher.enabled:
        dispatcher.enable()
        dispatcher.send("worker-online")
        state.logger.warn("Events enabled by remote.")
        return {"ok": "events enabled"}
    return {"ok": "events already enabled"}


def disable_events(state):
    dispatcher = state.listener.event_dispatcher
    if dispatcher.enabled:
        dispatcher.send("worker-offline")
        dispatcher.disable()
        state.logger.warn("Events disabled by remote.")
        return {"ok": "events disabled"}
    return {"ok": "events already disabled"}


def set_loglevel(state, loglevel=None):
    if loglevel is not None:
        if not isinstance(loglevel, int):
            loglevel = conf.LOG_LEVELS[loglevel.upper()]
        log.get_default_logger(loglevel=loglevel)
    return {"ok": loglevel}


def rate_limit(state, task_name, rate_limit, **kwargs):
    """Set new rate limit for a task type.

    See :attr:`celery.task.base.Task.rate_limit`.

    :param task_name: Type of task.
    :param rate_limit: New rate limit.

    """

    try:
        timeutils.rate(rate_limit)
    except ValueError, exc:
        return {"error": "Invalid rate limit string: %s" % exc}

    try:
        tasks[task_name].rate_limit = rate_limit
    except KeyError:
        state.logger.error("Rate limit attempt for unknown task %s" % (
            task_name, ))
        return {"error": "unknown task"}

    if conf.DISABLE_RATE_LIMITS:
        state.logger.error("Rate limit attempt, but rate limits disabled.")
        return {"error": "rate limits disabled"}

    state.listener.ready_queue.refresh()

    if not rate_limit:
        state.logger.warn("Disabled rate limits for tasks of type %s" % (
                            task_name, ))
        return {"ok": "rate limit disabled successfully"}

    state.logger.warn("New rate limit for tasks of type %s: %s." % (
                task_name, rate_limit))
    return {"ok": "new rate limit set successfully"}


def dump_schedule(state, safe=False, **kwargs):
    schedule = state.listener.eta_schedule
    if not schedule.queue:
        state.logger.info("--Empty schedule--")
        return []

    formatitem = lambda (i, item): "%s. %s pri%s %r" % (i,
            datetime.fromtimestamp(item["eta"]),
            item["priority"],
            item["item"])
    info = map(formatitem, enumerate(schedule.info()))
    state.logger.info("* Dump of current schedule:\n%s" % (
                            "\n".join(info, )))
    scheduled_tasks = []
    for item in schedule.info():
        scheduled_tasks.append({"eta": item["eta"],
                                "priority": item["priority"],
                                "request": item["item"].info(safe=safe)})
    return scheduled_tasks


def dump_reserved(state, safe=False, **kwargs):
    ready_queue = state.listener.ready_queue
    reserved = ready_queue.items
    if not reserved:
        state.logger.info("--Empty queue--")
        return []
    state.logger.info("* Dump of currently reserved tasks:\n%s" % (
                            "\n".join(map(repr, reserved), )))
    return [request.info(safe=safe)
            for request in reserved]


def dump_active(state, safe=False, **kwargs):
    return [request.info(safe=safe)
                for request in state.worker.active_requests]


@Panel.register
def stats(state, **kwargs):
    return {"total": state.worker.total_count,
            "pool": state.listener.pool.info}


def dump_revoked(state, **kwargs):
    return list(state.worker.revoked)


def dump_tasks(state, **kwargs):

    def _extract_info(task):
        fields = dict((field, str(getattr(task, field, None)))
                        for field in TASK_INFO_FIELDS
                            if getattr(task, field, None) is not None)
        info = map("=".join, fields.items())
        if not info:
            return task.name
        return "%s [%s]" % (task.name, " ".join(info))

    info = map(_extract_info, (tasks[task]
                                        for task in sorted(tasks.keys())))
    state.logger.warn("* Dump of currently registered tasks:\n%s" % (
                "\n".join(info)))

    return info


def ping(state, **kwargs):
    return "pong"


def shutdown(state, **kwargs):
    state.logger.critical("Got shutdown from remote.")
    raise SystemExit("Got shutdown from remote")
