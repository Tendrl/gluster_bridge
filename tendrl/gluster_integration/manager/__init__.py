import etcd
import gevent.event
import signal

from tendrl import gluster_integration
from tendrl.commons import TendrlNS
from tendrl.commons.event import Event
from tendrl.commons.message import Message
from tendrl.commons import manager as common_manager
from tendrl.gluster_integration import sds_sync
from tendrl.gluster_integration.gdeploy_wrapper.manager import \
    ProvisioningManager


class GlusterIntegrationManager(common_manager.Manager):
    def __init__(self):
        self._complete = gevent.event.Event()
        super(
            GlusterIntegrationManager,
            self
        ).__init__(
            NS.state_sync_thread
        )


def main():
    gluster_integration.GlusterIntegrationNS()
    TendrlNS()

    NS.type = "sds"
    NS.publisher_id = "gluster_integration"

    NS.state_sync_thread = sds_sync.GlusterIntegrationSdsSyncStateThread()

    NS.node_context.save()
    try:
        NS.tendrl_context = NS.tendrl_context.load()
        Event(
            Message(
                priority="debug",
                publisher=NS.publisher_id,
                payload={"message": "Integration %s is part of sds cluster"
                                    % NS.tendrl_context.integration_id
                         }
            )
        )
    except etcd.EtcdKeyNotFound:
        Event(
            Message(
                priority="debug",
                publisher=NS.publisher_id,
                payload={"message": "Node %s is not part of any sds cluster" %
                                    NS.node_context.node_id
                         }
            )
        )
        raise Exception(
            "Integration cannot be started,"
            " please Import or Create sds cluster"
            " in Tendrl and include Node %s" %
            NS.node_context.node_id
        )
    
    NS.tendrl_context.save()
    NS.gluster.definitions.save()
    NS.gluster.config.save()

    pm = ProvisioningManager("GdeployPlugin")
    NS.gdeploy_plugin = pm.get_plugin()

    m = GlusterIntegrationManager()
    m.start()

    complete = gevent.event.Event()

    def shutdown():
        Event(
            Message(
                priority="debug",
                publisher=NS.publisher_id,
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
