import logging

import gevent.event
import signal

from tendrl import gluster_integration
from tendrl.commons import TendrlNS
from tendrl.commons import manager as common_manager
from tendrl.gluster_integration import sds_sync
from tendrl.gluster_integration import central_store

LOG = logging.getLogger(__name__)


class GlusterIntegrationManager(common_manager.Manager):
    def __init__(self):
        self._complete = gevent.event.Event()
        super(
            GlusterIntegrationManager,
            self
        ).__init__(
            NS.state_sync_thread,
            NS.central_store_thread
        )


def main():
    gluster_integration.GlusterIntegrationNS()
    TendrlNS()

    NS.type = "sds"

    NS.central_store_thread = central_store.GlusterIntegrationEtcdCentralStore()
    NS.state_sync_thread = sds_sync.GlusterIntegrationSdsSyncStateThread()

    NS.node_context.save()
    NS.tendrl_context.save()
    NS.gluster_integration.definitions.save()
    NS.gluster_integration.config.save()
    NS.publisher_id = "gluster_integration"

    m = GlusterIntegrationManager()
    m.start()

    complete = gevent.event.Event()

    def shutdown():
        LOG.info("Signal handler: stopping")
        complete.set()

    gevent.signal(signal.SIGTERM, shutdown)
    gevent.signal(signal.SIGINT, shutdown)

    while not complete.is_set():
        complete.wait(timeout=1)


if __name__ == "__main__":
    main()
