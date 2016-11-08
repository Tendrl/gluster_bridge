import gevent.queue
from gevent import sleep
import pytest
from tendrl.gluster_bridge import gevent_util


def test_nosleep_mgr():
    test_nosleep_method1()
    with pytest.raises(gevent_util.ForbiddenYield):
        raise test_nosleep_method2()
    test_nosleep_method3()
    with pytest.raises(gevent_util.ForbiddenYield):
        raise test_nosleep_method4()
    setattr(gevent_util.getcurrent(), 'switch_out', "pytest")
    test_nosleep_method1()
    assert gevent_util.getcurrent().switch_out == "pytest"


# This should raise no exception (print doesn't yield)
@gevent_util.nosleep
def test_nosleep_method1():
    print("test print!")


# This should raise an exception when we try push to a fixed size queue
@gevent_util.nosleep
def test_nosleep_method2():
    smallq = gevent.queue.Queue(1)
    try:
        smallq.put(1)
        smallq.put(2)
    except gevent_util.ForbiddenYield as ex:
        return ex


# This should raise no exception when we try push to an unlimited
# size queue
@gevent_util.nosleep
def test_nosleep_method3():
    bigq = gevent.queue.Queue(0)
    for i in range(0, 10000):
        bigq.put(i)


# This should raise an exception on sleep
@gevent_util.nosleep
def test_nosleep_method4():
    try:
        sleep(0.1)
    except gevent_util.ForbiddenYield as ex:
        return ex
