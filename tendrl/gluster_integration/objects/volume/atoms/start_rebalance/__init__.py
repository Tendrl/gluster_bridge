import subprocess

from tendrl.commons.event import Event
from tendrl.commons.message import Message
from tendrl.gluster_integration import objects
from tendrl.gluster_integration.objects.volume import Volume


class StartRebalance(objects.GlusterIntegrationBaseAtom):
    obj = Volume
    def __init__(self, *args, **kwargs):
        super(StartRebalance, self).__init__(*args, **kwargs)

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
        base_cmd = [
            'gluster',
            'volume',
            'rebalance',
            self.parameters['Volume.volname']
        ]
        import pdb; pdb.set_trace()
        if "Volume.fix_layout" in self.parameters and \
            self.parameters['Volume.fix_layout'] is True:
            base_cmd.append('fix-layout')
        base_cmd.extend(['start', '--mode=script'])
        ret_val = subprocess.call(base_cmd)
        if ret_val != 0:
            Event(
                Message(
                    priority="info",
                    publisher=tendrl_ns.publisher_id,
                    payload={
                        "message": "Failed starting rebalance operation"
                    },
                    request_id=self.parameters["request_id"],
                    flow_id=self.parameters["flow_id"],
                    cluster_id=tendrl_ns.tendrl_context.integration_id,
                )
            )
            return False
        else:
            Event(
                Message(
                    priority="info",
                    publisher=tendrl_ns.publisher_id,
                    payload={
                        "message": "Successfully started rebalabce"
                    },
                    request_id=self.parameters["request_id"],
                    flow_id=self.parameters["flow_id"],
                    cluster_id=tendrl_ns.tendrl_context.integration_id,
                )
            )
            return True
