from tendrl.commons.event import Event
from tendrl.commons.message import Message
from tendrl.commons import objects
from tendrl.gluster_integration.objects.volume import Volume
from tendrl.gluster_integration.objects.volume import Volume
from tendrl.gluster_integration.objects.volume.atoms import \
    brick_lock_utils


class Expand(objects.BaseAtom):
    def __init__(self, *args, **kwargs):
        super(Expand, self).__init__(*args, **kwargs)

    def run(self):
        args = {}
        vol = Volume(vol_id=self.parameters['Volume.vol_id']).load()
        if self.parameters.get('Volume.replica_count') is not None:
            args.update({
                "replica_count": self.parameters.get('Volume.replica_count')
            })
            vol = Volume(vol_id=self.parameters['Volume.vol_id']).load()
            if vol.replica_count != self.parameters.get('Volume.replica_count'):
                args.update({"increase_replica_count": True})
        elif self.parameters.get('Volume.disperse_count') is not None:
            args.update({
                "disperse_count": self.parameters.get('Volume.disperse_count')
            })
        else:
            if int(vol.replica_count) > 1:
                args.update({
                    "replica_count": vol.replica_count
                })
            elif int(vol.disperse_count) > 1:
                args.update({
                    "disperse_count": vol.disperse_count
                })
                
        if self.parameters.get('Volume.force') is not None:
            args.update({
                "force": self.parameters.get('Volume.force')
            })

        # mark the bricks locked by the task_id so that they cannot be
        # picked by other create volume tasks
        Event(
            Message(
                priority="info",
                publisher=NS.publisher_id,
                payload={
                    "message": "Locking the bricks"
                },
                job_id=self.parameters["job_id"],
                flow_id=self.parameters["flow_id"],
                cluster_id=NS.tendrl_context.integration_id,
            )
        )
        brick_paths = brick_lock_utils.format_brick_paths(
            self.parameters.get("Volume.bricks")
        )
        lock_info = dict(
            node_id=NS.node_context.node_id,
            fqdn=NS.node_context.fqdn,
            tags=NS.node_context.tags,
            volume=vol.name,
            type=NS.type,
            job_id=self.parameters["job_id"],
            flow=self.__class__.__name__
        )
        brick_lock_utils.lock_bricks(brick_paths, lock_info)

        Event(
            Message(
                priority="info",
                publisher=NS.publisher_id,
                payload={
                    "message": "Expanding the volume %s" %
                    self.parameters['Volume.volname']
                },
                job_id=self.parameters["job_id"],
                flow_id=self.parameters["flow_id"],
                cluster_id=NS.tendrl_context.integration_id,
            )
        )
        if NS.gdeploy_plugin.expand_volume(
                self.parameters.get('Volume.volname'),
                self.parameters.get('Volume.bricks'),
                **args
        ):
            Event(
                Message(
                    priority="info",
                    publisher=NS.publisher_id,
                    payload={
                        "message": "Expanded the volume %s" %
                        self.parameters['Volume.volname']
                    },
                    job_id=self.parameters["job_id"],
                    flow_id=self.parameters["flow_id"],
                    cluster_id=NS.tendrl_context.integration_id,
                )
            )
            return True
        else:
            Event(
                Message(
                    priority="error",
                    publisher=NS.publisher_id,
                    payload={
                        "message": "Volume expansion failed for volume %s" %
                        self.parameters['Volume.volname']
                    },
                    job_id=self.parameters["job_id"],
                    flow_id=self.parameters["flow_id"],
                    cluster_id=NS.tendrl_context.integration_id,
                )
            )
            # Unlock the bricks so that they can be used later
            Event(
                Message(
                    priority="info",
                    publisher=NS.publisher_id,
                    payload={
                        "message": "Un-locking the bricks"
                    },
                    job_id=self.parameters["job_id"],
                    flow_id=self.parameters["flow_id"],
                    cluster_id=NS.tendrl_context.integration_id,
                )
            )
            brick_lock_utils(brick_paths)

            return False
