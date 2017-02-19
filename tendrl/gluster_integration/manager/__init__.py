import gevent.event
import signal

from tendrl.commons.event import Event
from tendrl.commons.message import Message

from tendrl.commons import manager as common_manager
from tendrl.gluster_integration import sds_sync
from tendrl.gluster_integration import central_store


class GlusterIntegrationManager(common_manager.Manager):
    def __init__(self):
        self._complete = gevent.event.Event()
        super(
            GlusterIntegrationManager,
            self
        ).__init__(
            tendrl_ns.state_sync_thread,
            tendrl_ns.central_store_thread
        )


def main():
    tendrl_ns.publisher_id = "gluster_integration"
    tendrl_ns.central_store_thread = central_store.GlusterIntegrationEtcdCentralStore()
    tendrl_ns.state_sync_thread = sds_sync.GlusterIntegrationSdsSyncStateThread()

    tendrl_ns.node_context.save()
    tendrl_ns.tendrl_context.save()
    tendrl_ns.definitions.save()
    tendrl_ns.config.save()

    m = GlusterIntegrationManager()
    m.start()

    complete = gevent.event.Event()

    def shutdown():
        Event(
            Message(
                priority="info",
                publisher=tendrl_ns.publisher_id,
                payload={"message": "Signal handler: stopping"}
            )
        )
        complete.set()

    gevent.signal(signal.SIGTERM, shutdown)
    gevent.signal(signal.SIGINT, shutdown)

    while not complete.is_set():
        complete.wait(timeout=1)


if __name__ == "__main__":
    main()
