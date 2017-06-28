import etcd
import subprocess

from tendrl.commons.event import Event
from tendrl.commons.message import Message
from tendrl.commons import objects
from tendrl.gluster_integration.objects.volume import Volume


class RebalanceNotRunning(objects.BaseAtom):
    obj = Volume
    def __init__(self, *args, **kwargs):
        super(RebalanceNotRunning, self).__init__(*args, **kwargs)

    def run(self):
        Event(
            Message(
                priority="info",
                publisher=NS.publisher_id,
                payload={
                    "message": "Checking if rebalance is not running"
                },
                job_id=self.parameters["job_id"],
                flow_id=self.parameters["flow_id"],
                cluster_id=NS.tendrl_context.integration_id,
            )
        )
        try:
            vol_rebal_det = NS._int.client.read(
                'clusters/%s/Volumes/%s/RebalanceDetails' % (
                    NS.tendrl_context.integration_id,
                    self.parameters['Volume.vol_id']
                )
            )
            for key in vol_rebal_det.leaves:
                obj = NS.gluster.objects.RebalanceDetails(
                    vol_id=self.parameters['Volume.vol_id'],
                    node_id=key.split('/')[-1]
                ).load()
                if obj.rebal_status is not None:
                    if obj.rebal_status == "in progress":
                        Event(
                            Message(
                                priority="info",
                                publisher=NS.publisher_id,
                                payload={
                                    "message": "Volume rebalance status is running on node %s" %
                                    obj.node_id
                                },
                                job_id=self.parameters["job_id"],
                                flow_id=self.parameters["flow_id"],
                                cluster_id=NS.tendrl_context.integration_id,
                            )
                        )
                        return False
            return True
        except etcd.EtcdKeyNotFound as ex:
            Event(
                Message(
                    priority="error",
                    publisher=NS.publisher_id,
                    payload={
                        "message": "Volume %s not found" %
                        self.parameters['Volume.volname']
                    },
                    job_id=self.parameters["job_id"],
                    flow_id=self.parameters["flow_id"],
                    cluster_id=NS.tendrl_context.integration_id,
                )
            )
            return False
