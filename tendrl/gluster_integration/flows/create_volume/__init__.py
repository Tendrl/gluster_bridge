from tendrl.commons.event import Event
from tendrl.commons.message import Message
from tendrl.gluster_integration import flows


class CreateVolume(flows.GlusterIntegrationBaseFlow):
    def run(self):
        Event(
            Message(
                priority="info",
                publisher=tendrl_ns.publisher_id,
                payload={
                    "message": "Starting creation flow for volume %s" %
                    self.parameters['Volume.volname']
                },
                job_id=self.job_id,
                flow_id=self.uuid,
                cluster_id=tendrl_ns.tendrl_context.integration_id,
            )
        )

        super(CreateVolume, self).run()
