import etcd
import json
import subprocess

from tendrl.commons.event import Event
from tendrl.commons.message import Message
from tendrl.commons import objects
from tendrl.commons.objects import AtomExecutionFailedError
from tendrl.gluster_integration.objects.volume import Volume
from tendrl.gluster_integration.objects.volume import Volume
from tendrl.gluster_integration.objects.volume.atoms import \
    brick_lock_utils


class Create(objects.BaseAtom):
    obj = Volume
    def __init__(self, *args, **kwargs):
        super(Create, self).__init__(*args, **kwargs)

    def run(self):
        args = {}
        if self.parameters.get('Volume.replica_count') is not None:
            args.update({
                "replica_count": self.parameters.get('Volume.replica_count')
            })
        if self.parameters.get('Volume.transport') is not None:
            args.update({
                "transport": self.parameters.get('Volume.transport')
            })
        if self.parameters.get('Volume.disperse_count') is not None:
            args.update({
                "disperse_count": self.parameters.get('Volume.disperse_count')
            })
        if self.parameters.get('Volume.redundancy_count') is not None:
            args.update({
                "redundancy_count": self.parameters.get(
                    'Volume.redundancy_count'
                )
            })
        if self.parameters.get('Volume.tuned_profile') is not None:
            args.update({
                "tuned_profile": self.parameters.get('Volume.tuned_profile')
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
            volume=self.parameters.get("Volume.volname"),
            type=NS.type,
            job_id=self.parameters["job_id"],
            flow=self.__class__.__name__
        )
        brick_lock_utils.lock_bricks(brick_paths, lock_info)

        # Create the volume now
        Event(
            Message(
                priority="info",
                publisher=NS.publisher_id,
                payload={
                    "message": "Creating the volume %s" %
                    self.parameters['Volume.volname']
                },
                job_id=self.parameters["job_id"],
                flow_id=self.parameters["flow_id"],
                cluster_id=NS.tendrl_context.integration_id,
            )
        )

        if NS.gdeploy_plugin.create_volume(
                self.parameters.get('Volume.volname'),
                self.parameters.get('Volume.bricks'),
                **args
        ):
            Event(
                Message(
                    priority="info",
                    publisher=NS.publisher_id,
                    payload={
                        "message": "Created the volume %s" %
                        self.parameters['Volume.volname']
                    },
                    job_id=self.parameters["job_id"],
                    flow_id=self.parameters["flow_id"],
                    cluster_id=NS.tendrl_context.integration_id,
                )
            )

            # mark the bricks that were used to create this volume as
            # used bricks so that they are not consumed again
            for sub_vol in self.parameters.get('Volume.bricks'):
                for brick in sub_vol:
                    ip = brick.keys()[0]
                    brick_path = brick.values()[0]
                    try:
                        # Find node id using ip
                        node_id = NS._int.client.read("indexes/ip/%s" % ip).value
                    except etcd.EtcdKeyNotFound:
                        # Find node id using hostname
                        nodes = NS._int.client.read("nodes/")
                        for node in nodes.leaves:
                            hostname = NS._int.client.read(
                                "%s/NodeContext/fqdn" % node.key).value
                            if hostname == ip:
                                node_id = node.key.split("/")[-1]
                    key = "nodes/%s/NodeContext/fqdn" % node_id
                    host = NS._int.client.read(key).value
                    brick_path = host + ":" + brick_path
                    NS._int.wclient.delete(
                        ("clusters/%s/nodes/%s/GlusterBricks/free/%s") % (
                            NS.tendrl_context.integration_id,
                            node_id,
                            brick_path.replace("/","_")
                        )
                    )
                    NS._int.wclient.write(
                        ("clusters/%s/nodes/%s/GlusterBricks/used/%s") % (
                            NS.tendrl_context.integration_id,
                            node_id,
                            brick_path.replace("/","_")
                        ),
                        ""
                    )
                    NS._int.wclient.write(
                        ("clusters/%s/nodes/%s/GlusterBricks/all/%s/volume_name") % (
                            NS.tendrl_context.integration_id,
                            node_id,
                            brick_path.replace("/","_")
                        ),
                        self.parameters['Volume.volname']
                    )
        else:
            Event(
                Message(
                    priority="error",
                    publisher=NS.publisher_id,
                    payload={
                        "message": "Volume creation failed for volume %s" %
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

        return True
