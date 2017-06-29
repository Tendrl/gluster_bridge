import etcd
import json
import subprocess

from tendrl.commons.event import Event
from tendrl.commons.message import Message
from tendrl.commons import objects
from tendrl.commons.objects import AtomExecutionFailedError
from tendrl.gluster_integration.objects.volume import Volume
from tendrl.gluster_integration.objects.volume.atoms import \
    brick_lock_utils


class BricksLocked(objects.BaseAtom):
    def __init__(self, *args, **kwargs):
        super(BricksLocked, self).__init__(*args, **kwargs)

    def run(self):
        Event(
            Message(
                priority="info",
                publisher=NS.publisher_id,
                payload={
                    "message": "Checking if bricks are locked already"
                },
                job_id=self.parameters["job_id"],
                flow_id=self.parameters["flow_id"],
                cluster_id=NS.tendrl_context.integration_id,
            )
        )
        brick_locked_status = {}
        locked_bricks = []
        brick_paths = brick_lock_utils.format_brick_paths(
            self.parameters.get('Volume.bricks')
        )
        for brick_path in brick_paths:
            brick_lock_key = "clusters/%s/nodes/%s/GlusterBricks/all/%s/locked_by" % \
                (
                    NS.tendrl_context.integration_id,
                    node_id,
                    brick_path.replace("/","_")
                )
            try:
                lock_detail = json.loads(NS._int.wclient.read(brick_lock_key).value)
                Event(
                    Message(
                        priority="info",
                        publisher=NS.publisher_id,
                        payload={
                            "message": "Brick %s already locked by (%s, %s)" %
                            (brick_path, lock_detail['flow'], lock_detail['job_id'])
                        },
                        job_id=self.parameters["job_id"],
                        flow_id=self.parameters["flow_id"],
                        cluster_id=NS.tendrl_context.integration_id,
                    )
                )
                brick_locked_status[brick_lock_key] = True
                locked_bricks.append(brick_path)
            except etcd.EtcdKeyNotFound:
                brick_locked_status[brick_lock_key] = False

        brick_locked = {key: status for key, status in brick_locked_status.iteritems() if status is True}
        if bool(brick_locked):
            raise AtomExecutionFailedError(
                "Bricks %s already locked by other jobs" % str(locked_bricks)
            )

        return True
