import subprocess

from tendrl.commons.event import Event
from tendrl.commons.message import Message
from tendrl.gluster_integration import objects
from tendrl.gluster_integration.objects.volume import Volume


class Create(objects.GlusterIntegrationBaseAtom):
    obj = Volume
    def __init__(self, *args, **kwargs):
        super(Create, self).__init__(*args, **kwargs)

    def run(self):
        cmd = [
            'gluster',
            'volume',
            'create',
            self.parameters.get('Volume.volname')
        ]
        if self.parameters.get('Volume.stripe_count') is not None:
            cmd.append('stripe')
            cmd.append(str(self.parameters.get('Volume.stripe_count')))
        elif self.parameters.get('Volume.replica_count') is not None:
            cmd.append('replica')
            cmd.append(str(self.parameters.get('Volume.replica_count')))
            if self.parameters.get('Volume.arbiter_count') is not None:
                cmd.append('arbiter')
                cmd.append(str(self.parameters.get('Volume.arbiter_count')))
        elif self.parameters.get('Volume.disperse_count') is not None:
            cmd.append('disperse')
            cmd.append(str(self.parameters.get('Volume.disperse_count')))
        elif self.parameters.get('Volume.redundancy_count') is not None:
            cmd.append('redundancy')
            cmd.append(str(self.parameters.get('Volume.redundancy_count')))
        elif self.parameters.get('Volume.disperse_data_count') is not None:
            cmd.append('disperse-data')
            cmd.append(str(self.parameters.get('Volume.disperse_data_count')))
        if self.parameters.get('Volume.transport'):
            cmd.append('transport')
            cmd.append(','.join(self.parameters.get('Volume.transport')))
        cmd.extend(self.parameters.get('Volume.bricks'))
        cmd.append('force')
        cmd.append('--mode=script')
        Event(
            Message(
                priority="info",
                publisher=tendrl_ns.publisher_id,
                payload={
                    "message": "Creating the volume %s" %
                    self.parameters['Volume.volname']
                },
                job_id=self.parameters["job_id"],
                flow_id=self.parameters["flow_id"],
                cluster_id=tendrl_ns.tendrl_context.integration_id,
            )
        )

        subprocess.call(cmd)
        Event(
            Message(
                priority="info",
                publisher=tendrl_ns.publisher_id,
                payload={
                    "message": "Created the volume %s" %
                    self.parameters['Volume.volname']
                },
                job_id=self.parameters["job_id"],
                flow_id=self.parameters["flow_id"],
                cluster_id=tendrl_ns.tendrl_context.integration_id,
            )
        )
        subprocess.call(
            [
                'gluster',
                'volume',
                'start',
                self.parameters.get('Volume.volname'),
                '--mode=script'
            ]
        )
        Event(
            Message(
                priority="info",
                publisher=tendrl_ns.publisher_id,
                payload={
                    "message": "Started the volume %s" %
                    self.parameters['Volume.volname']
                },
                job_id=self.parameters["job_id"],
                flow_id=self.parameters["flow_id"],
                cluster_id=tendrl_ns.tendrl_context.integration_id,
            )
        )

        return True
