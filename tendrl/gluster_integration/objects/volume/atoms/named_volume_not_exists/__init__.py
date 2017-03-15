import etcd

from tendrl.commons.event import Event
from tendrl.commons.message import Message
from tendrl.gluster_integration import objects
from tendrl.gluster_integration.objects.volume import Volume


class NamedVolumeNotExists(objects.GlusterIntegrationBaseAtom):
    obj = Volume
    def __init__(self, *args, **kwargs):
        super(NamedVolumeNotExists, self).__init__(*args, **kwargs)

    def run(self):
        Event(
            Message(
                priority="info",
                publisher=tendrl_ns.publisher_id,
                payload={
                    "message": "Checking if volume %s doesnt exist" %
                    self.parameters['Volume.volname']
                },
                request_id=self.parameters["request_id"],
                flow_id=self.parameters["flow_id"],
                cluster_id=tendrl_ns.tendrl_context.integration_id,
            )
        )
        try:
            volumes = tendrl_ns.etcd_orm.client.read(
                "clusters/%s/Volumes" % tendrl_ns.tendrl_context.integration_id
            )
        except etcd.EtcdKeyNotFound:
            # No volumes present yet for cluster, return true
            return True

        for volume in volumes._children:
            fetched_volume = Volume(
                vol_id=volume['key'].split('/')[-1]
            ).load()
            if fetched_volume.name == \
                self.parameters['Volume.volname']:
                Event(
                    Message(
                        priority="info",
                        publisher=tendrl_ns.publisher_id,
                        payload={
                            "message": "Volume %s already exists" %
                            self.parameters['Volume.volname']
                        },
                        request_id=self.parameters["request_id"],
                        flow_id=self.parameters["flow_id"],
                        cluster_id=tendrl_ns.tendrl_context.integration_id,
                    )
                )
                return False

        return True
