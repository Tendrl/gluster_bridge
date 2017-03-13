import etcd
import subprocess

from tendrl.commons.event import Event
from tendrl.commons.message import Message
from tendrl.gluster_integration import objects
from tendrl.gluster_integration.objects.volume import Volume


class RebalanceNotRunning(objects.GlusterIntegrationBaseAtom):
    obj = Volume
    def __init__(self, *args, **kwargs):
        super(RebalanceNotRunning, self).__init__(*args, **kwargs)

    def run(self):
        Event(
            Message(
                priority="info",
                publisher=tendrl_ns.publisher_id,
                payload={
                    "message": "Checking if rebalance is not running"
                },
                request_id=self.parameters["request_id"],
                flow_id=self.parameters["flow_id"],
                cluster_id=tendrl_ns.tendrl_context.integration_id,
            )
        )
        try:
            fetched_volume = Volume(
                vol_id=self.parameters['Volume.vol_id']
            ).load()
            if fetched_volume.rebal_status is not None:
                if fetched_volume.rebal_status == "not applicable" or\
                    fetched_volume.rebal_status == "completed":
                    return True
                if fetched_volume.rebal_status == "in progress":
                    return False
                Event(
                    Message(
                        priority="info",
                        publisher=tendrl_ns.publisher_id,
                        payload={
                            "message": "Volume rebalance status is %s" %
                            fetched_volume.rebal_status
                        },
                        request_id=self.parameters["request_id"],
                        flow_id=self.parameters["flow_id"],
                        cluster_id=tendrl_ns.tendrl_context.integration_id,
                    )
                )
                return False
            else:
                return True
        except etcd.EtcdKeyNotFound as ex:
            Event(
                Message(
                    priority="info",
                    publisher=tendrl_ns.publisher_id,
                    payload={
                        "message": "Volume %s not found" %
                        self.parameters['Volume.volname']
                    },
                    request_id=self.parameters["request_id"],
                    flow_id=self.parameters["flow_id"],
                    cluster_id=tendrl_ns.tendrl_context.integration_id,
                )
            )
            return False
