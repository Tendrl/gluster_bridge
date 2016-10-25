from contextlib import contextmanager
from functools import wraps
from gevent import getcurrent


class ForbiddenYield(Exception):
    pass


@contextmanager
def nosleep_mgr():
    old_switch_out = getattr(getcurrent(), 'switch_out', None)

    def asserter():
        raise ForbiddenYield("Context switch during `nosleep` region!")

    getcurrent().switch_out = asserter
    try:
        yield
    finally:
        if old_switch_out is not None:
            getcurrent().switch_out = old_switch_out
        else:
            del getcurrent().switch_out


def nosleep(func):
    """This decorator is used to assert that no geven greenlet yields

    occur in the decorated function.

    """
    @wraps(func)
    def wrapped(*args, **kwargs):
        with nosleep_mgr():
            return func(*args, **kwargs)

    return wrapped
