import etcd

from tendrl.commons.event import Event
from tendrl.commons.message import Message
from tendrl.commons import objects
from tendrl.gluster_integration.objects.volume import Volume


class VolumeNotExists(objects.BaseAtom):
    obj = Volume
    def __init__(self, *args, **kwargs):
        super(VolumeNotExists, self).__init__(*args, **kwargs)

    def run(self):
        Event(
            Message(
                priority="info",
                publisher=NS.publisher_id,
                payload={
                    "message": "Checking if volume %s doesnt exist" %
                    self.parameters['Volume.volname']
                },
                job_id=self.parameters["job_id"],
                flow_id=self.parameters["flow_id"],
                cluster_id=NS.tendrl_context.integration_id,
            )
        )
        try:
            NS._int.client.read(
                'clusters/%s/Volumes/%s' % (
                    NS.tendrl_context.integration_id,
                    self.parameters['Volume.vol_id']
                )
            )
        except etcd.EtcdKeyNotFound:
            Event(
                Message(
                    priority="warning",
                    publisher=NS.publisher_id,
                    payload={
                        "message": "Volume %s doesnt exist" %
                        self.parameters['Volume.volname']
                    },
                    job_id=self.parameters["job_id"],
                    flow_id=self.parameters["flow_id"],
                    cluster_id=NS.tendrl_context.integration_id,
                )
            )
            return True

        return False
