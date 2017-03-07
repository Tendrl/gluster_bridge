from tendrl.commons.event import Event
from tendrl.commons.message import Message
from tendrl.gluster_integration import flows
from tendrl.gluster_integration.objects.volume import Volume


class DeleteVolume(flows.GlusterIntegrationBaseFlow):
    obj = Volume
    def __init__(self, *args, **kwargs):
        super(DeleteVolume, self).__init__(*args, **kwargs)

    def run(self):
        Event(
            Message(
                priority="info",
                publisher=tendrl_ns.publisher_id,
                payload={
                    "message": "Starting deletion flow for volume %s" %
                    self.parameters['Volume.volname']
                },
                job_id=self.job_id,
                flow_id=self.uuid,
                cluster_id=tendrl_ns.tendrl_context.integration_id,
            )
        )

        super(DeleteVolume, self).run()
