from tendrl.commons.event import Event
from tendrl.commons.message import Message
from tendrl.commons import objects
from tendrl.gluster_integration.objects.volume import Volume


class NamedVolumeNotExists(objects.BaseAtom):
    obj = Volume
    def __init__(self, *args, **kwargs):
        super(NamedVolumeNotExists, self).__init__(*args, **kwargs)

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
        volumes = NS.etcd_orm.client.read(
            "clusters/%s/Volumes" % NS.tendrl_context.integration_id
        )
        for volume in volumes._children:
            fetched_volume = Volume(
                vol_id=volume['key'].split('/')[-1]
            ).load()
            if fetched_volume.name == \
                self.parameters['Volume.volname']:
                Event(
                    Message(
                        priority="info",
                        publisher=NS.publisher_id,
                        payload={
                            "message": "Volume %s already exists" %
                            self.parameters['Volume.volname']
                        },
                        job_id=self.parameters["job_id"],
                        flow_id=self.parameters["flow_id"],
                        cluster_id=NS.tendrl_context.integration_id,
                    )
                )
                return False

        return True
