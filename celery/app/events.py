from celery.events import EventReceiver, EventDispatcher


class Events(object):
    EventReceiver = EventReceiver
    EventDispatcher = EventDispatcher

    def __init__(self, app=None):
        self.app = app

    def Receiver(self, connection, handlers=None, routing_key="#",
            node_id=None):
        return self.EventReceiver(connection,
                    handlers=handlers, routing_key=routing_key,
                    node_id=node_id, app=self.app)

    def Dispatcher(self, connection=None, hostname=None, enabled=True,
            channel=None, buffer_while_offline=True):
        return self.EventDispatcher(connection,
                    hostname=hostname, enabled=enabled,
                    channel=channel, app=self.app)

    def State(self):
        from celery.events.state import State as _State
        return _State()
