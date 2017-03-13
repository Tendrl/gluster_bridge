from tendrl.commons.event import Event
from tendrl.commons.message import Message
from tendrl.gluster_integration import flows
from tendrl.gluster_integration.objects.volume import Volume


class StartVolumeRebalance(flows.GlusterIntegrationBaseFlow):
    obj = Volume
    def __init__(self, *args, **kwargs):
        super(StartVolumeRebalance, self).__init__(*args, **kwargs)

    def run(self):
        Event(
            Message(
                priority="info",
                publisher=tendrl_ns.publisher_id,
                payload={
                    "message": "Starting rebalance for volume %s" %
                    self.parameters['Volume.volname']
                },
                request_id=self.parameters["request_id"],
                flow_id=self.parameters["flow_id"],
                cluster_id=tendrl_ns.tendrl_context.integration_id,
            )
        )

        super(StartVolumeRebalance, self).run()
