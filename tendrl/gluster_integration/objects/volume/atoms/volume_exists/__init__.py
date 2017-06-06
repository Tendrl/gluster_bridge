
from tendrl.commons.event import Event
from tendrl.commons.message import Message
from tendrl.commons import objects
from tendrl.gluster_integration.objects.volume import Volume


class VolumeExists(objects.BaseAtom):
    def __init__(self, *args, **kwargs):
        super(VolumeExists, self).__init__(*args, **kwargs)

    def run(self):
        Event(
            Message(
                priority="info",
                publisher=NS.publisher_id,
                payload={
                    "message": "Checking if volume %s exists" %
                    self.parameters['Volume.volname']
                },
                job_id=self.parameters["job_id"],
                flow_id=self.parameters["flow_id"],
                cluster_id=NS.tendrl_context.integration_id,
            )
        )
        if Volume(vol_id=self.parameters['Volume.vol_id']).exists():
            return True
        else:
            Event(
                Message(
                    priority="error",
                    publisher=NS.publisher_id,
                    payload={
                        "message": "Volume %s doesnt exist" %
                        self.parameters["Volume.volname"]
                    },
                    job_id=self.parameters["job_id"],
                    flow_id=self.parameters['flow_id'],
                    cluster_id=NS.tendrl_context.integration_id,
                )
            )
            return False
