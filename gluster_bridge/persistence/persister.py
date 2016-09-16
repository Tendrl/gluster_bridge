import datetime
from etcdobj import Server as etcd_server
import gevent.event
import gevent.greenlet
import gevent.queue

try:
    import msgpack
except ImportError:
    msgpack = None

from tendrl.gluster_bridge import config
from tendrl.gluster_bridge.log import log
from tendrl.gluster_bridge.persistence.sync_objects import SyncObject


CLUSTER_MAP_RETENTION = datetime.timedelta(
    seconds=int(config.get('bridge', 'cluster_map_retention')))


class deferred_call(object):

    def __init__(self, fn, args, kwargs):
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def call_it(self):
        self.fn(*self.args, **self.kwargs)


class Persister(gevent.greenlet.Greenlet):
    """Asynchronously persist a queue of updates.  This is for use by classes

    that maintain the primary copy of state in memory, but also lazily update

    the DB so that they can recover from it on restart.

    """

    def __init__(self):
        super(Persister, self).__init__()

        self._queue = gevent.queue.Queue()
        self._complete = gevent.event.Event()

        self._store = self.get_store()

    def __getattribute__(self, item):
        """Wrap functions with logging

        """
        if item.startswith('_'):
            return object.__getattribute__(self, item)
        else:
            try:
                return object.__getattribute__(self, item)
            except AttributeError:
                try:
                    attr = object.__getattribute__(self, "_%s" % item)
                    if callable(attr):
                        def defer(*args, **kwargs):
                            dc = deferred_call(attr, args, kwargs)
                            try:
                                dc.call_it()
                            except Exception as ex:
                                log.exception(
                                    "Persister exception persisting "
                                    "data: %s" % (dc.fn,)
                                )
                                log.exception(ex)
                        return defer
                    else:
                        return object.__getattribute__(self, item)
                except AttributeError:
                    return object.__getattribute__(self, item)

    def _update_sync_object(self, updated, data):
        self._store.save(SyncObject(updated=updated, data=data))

    def _update_peer(self, peer):
        self._store.save(peer)

    def _update_volume(self, vol):
        self._store.save(vol)

    def _update_brick(self, brick):
        self._store.save(brick)

    def _save_events(self, events):
        for event in events:
            self._store.save(event)

    def _run(self):
        log.info("Persister listening")

        while not self._complete.is_set():
            gevent.sleep(0.1)
            pass

    def stop(self):
        self._complete.set()

    def get_store(self):
        etcd_kwargs = {'port': int(config.get("bridge", "etcd_port")),
                       'host': config.get("bridge", "etcd_connection")}
        return etcd_server(etcd_kwargs=etcd_kwargs)
