from kombu import pidbox

from celery.datastructures import AttributeDict
from celery.worker.control import builtins

handlers = {"shutdown": builtins.shutdown,
            "ping": builtins.ping,
            "dump_tasks": builtins.dump_tasks,
            "dump_revoked": builtins.dump_revoked,
            "dump_active": builtins.dump_active,
            "dump_reserved": builtins.dump_reserved,
            "dump_schedule": builtins.dump_schedule,
            "stats": builtins.stats,
            "rate_limit": builtins.rate_limit,
            "set_loglevel": builtins.set_loglevel,
            "enable_events": builtins.enable_events,
            "disable_events": builtins.disable_events,
            "revoke": builtins.revoke}

mailbox = pidbox.Mailbox("celeryd", type="broadcast", handlers=handlers)


def get_mailbox(**state):
    return mailbox.clone(AttributeDict(state))
