from __future__ import absolute_import, with_statement

from kombu.utils import cached_property

from celery import registry
from celery.app import app_or_default
from celery.datastructures import AttributeDict
from celery.utils import gen_unique_id
from celery.utils.compat import UserList


class subtask(AttributeDict):
    """Class that wraps the arguments and execution options
    for a single task invocation.

    Used as the parts in a :class:`TaskSet` or to safely
    pass tasks around as callbacks.

    :param task: Either a task class/instance, or the name of a task.
    :keyword args: Positional arguments to apply.
    :keyword kwargs: Keyword arguments to apply.
    :keyword options: Additional options to
      :func:`celery.execute.apply_async`.

    Note that if the first argument is a :class:`dict`, the other
    arguments will be ignored and the values in the dict will be used
    instead.

        >>> s = subtask("tasks.add", args=(2, 2))
        >>> subtask(s)
        {"task": "tasks.add", args=(2, 2), kwargs={}, options={}}

    """

    def __init__(self, task=None, args=None, kwargs=None, options=None, **ex):
        init = super(subtask, self).__init__

        if isinstance(task, dict):
            return init(task)  # works like dict(d)

        # Also supports using task class/instance instead of string name.
        try:
            task_name = task.name
        except AttributeError:
            task_name = task

        init(task=task_name, args=tuple(args or ()),
                             kwargs=dict(kwargs or {}, **ex),
                             options=options or {})

    def delay(self, *argmerge, **kwmerge):
        """Shortcut to `apply_async(argmerge, kwargs)`."""
        return self.apply_async(args=argmerge, kwargs=kwmerge)

    def apply(self, args=(), kwargs={}, **options):
        """Apply this task locally."""
        # For callbacks: extra args are prepended to the stored args.
        args = tuple(args) + tuple(self.args)
        kwargs = dict(self.kwargs, **kwargs)
        options = dict(self.options, **options)
        return self.type.apply(args, kwargs, **options)

    def apply_async(self, args=(), kwargs={}, **options):
        """Apply this task asynchronously."""
        # For callbacks: extra args are prepended to the stored args.
        args = tuple(args) + tuple(self.args)
        kwargs = dict(self.kwargs, **kwargs)
        options = dict(self.options, **options)
        return self.type.apply_async(args, kwargs, **options)

    def get_type(self):
        return self.type

    def __reduce__(self):
        # for serialization, the task type is lazily loaded,
        # and not stored in the dict itself.
        return (self.__class__, (dict(self), ), None)

    def __repr__(self, kwformat=lambda i: "%s=%r" % i, sep=', '):
        kw = self["kwargs"]
        return "%s(%s%s%s)" % (self["task"], sep.join(map(repr, self["args"])),
                kw and sep or "", sep.join(map(kwformat, kw.iteritems())))

    @cached_property
    def type(self):
        return registry.tasks[self.task]


class TaskSet(UserList):
    """A task containing several subtasks, making it possible
    to track how many, or when all of the tasks have been completed.

    :param tasks: A list of :class:`subtask` instances.

    Example::

        >>> urls = ("http://cnn.com/rss", "http://bbc.co.uk/rss")
        >>> taskset = TaskSet(refresh_feed.subtask((url, )) for url in urls)
        >>> taskset_result = taskset.apply_async()
        >>> list_of_return_values = taskset_result.join()  # *expensive*

    """
    #: Total number of subtasks in this set.
    total = None

    def __init__(self, tasks=None, app=None, Publisher=None):
        self.app = app_or_default(app)
        self.data = list(tasks or [])
        self.total = len(self.tasks)
        self.Publisher = Publisher or self.app.amqp.TaskPublisher

    def apply_async(self, connection=None, connect_timeout=None,
            publisher=None):
        """Apply taskset."""
        app = self.app
        if app.conf.CELERY_ALWAYS_EAGER:
            return self.apply()

        with app.default_connection(connection, connect_timeout) as conn:
            taskset_id = gen_unique_id()
            pub = publisher or self.Publisher(conn)
            try:
                results = [task.apply_async(taskset_id=taskset_id,
                                            publisher=pub)
                            for task in self.tasks]
            finally:
                if not publisher:  # created by us.
                    pub.close()

            return app.TaskSetResult(taskset_id, results)

    def apply(self):
        """Applies the taskset locally by blocking until all tasks return."""
        setid = gen_unique_id()
        return self.app.TaskSetResult(setid, [task.apply(taskset_id=setid)
                                                for task in self.tasks])

    @property
    def tasks(self):
        return self.data
