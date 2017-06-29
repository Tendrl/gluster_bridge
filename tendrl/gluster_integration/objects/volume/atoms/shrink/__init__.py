import etcd

from tendrl.commons.event import Event
from tendrl.commons.message import Message
from tendrl.commons import objects
from tendrl.gluster_integration.objects.volume import Volume


class Shrink(objects.BaseAtom):
    def __init__(self, *args, **kwargs):
        super(Shrink, self).__init__(*args, **kwargs)

    def run(self):
        args = {}
        vol = Volume(vol_id=self.parameters['Volume.vol_id']).load()
        if self.parameters.get('Volume.replica_count') is not None:
            args.update({
                "replica_count": self.parameters.get('Volume.replica_count')
            })
            if vol.replica_count != self.parameters.get('Volume.replica_count'):
                args.update({"decrease_replica_count": True})
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

        action = self.parameters.get('Volume.action')

        Event(
            Message(
                priority="info",
                publisher=NS.publisher_id,
                payload={
                    "message": "Shrinking the volume %s" %
                    self.parameters['Volume.volname']
                },
                job_id=self.parameters["job_id"],
                flow_id=self.parameters["flow_id"],
                cluster_id=NS.tendrl_context.integration_id,
            )
        )

        if NS.gdeploy_plugin.shrink_volume(
                self.parameters.get('Volume.volname'),
                self.parameters.get('Volume.bricks'),
                action,
                **args
        ):
            Event(
                Message(
                    priority="info",
                    publisher=NS.publisher_id,
                    payload={
                        "message": "Shrinked the volume %s" %
                        self.parameters['Volume.volname']
                    },
                    job_id=self.parameters["job_id"],
                    flow_id=self.parameters["flow_id"],
                    cluster_id=NS.tendrl_context.integration_id,
                )
            )

            if action != "commit" and not args.has_key("decrease_replica_count"):
                return True
            try:
                # Delete the bricks from central store
                # Acquire lock before deleting the bricks from etcd
                # We are blocking till we acquire the lock
                # the lock will live for 60 sec after which it will be released.
                lock = etcd.Lock(NS._int.wclient, 'volume')
                
                while not lock.is_acquired:
                    try:
                        # with ttl set, lock will be blocked only for 60 sec
                        # after which it will raise lock_expired exception.
                        # if this is raised, we have to retry for lock
                        lock.acquire(blocking=True,lock_ttl=60)
                        if lock.is_acquired:
                            # renewing lock as we are not sure, how long we
                            # were blocked before the lock was given.
                            # NOTE: blocked time also counts as ttl
                            lock.acquire(lock_ttl=60)
                    except etcd.EtcdLockExpired:
                        continue
                subvolumes = NS._int.client.read(
                    "clusters/%s/Volumes/%s/Bricks" % (
                        NS.tendrl_context.integration_id,
                        self.parameters['Volume.vol_id']
                    ),
                )
                brick_details = {}
                for subvolume in subvolumes.leaves:
                    subvolume_id = subvolume.key.split("/")[-1]
                    bricks = NS._int.client.read(
                        subvolume.key
                    )
                    for brick in bricks.leaves:
                        brick_details[brick.key.split("/")[-1]] = subvolume_id
                for sub_vol in self.parameters.get('Volume.bricks'):
                    for b in sub_vol:
                        brick_name = b.keys()[0] + ":" + b.values()[0].replace("/", "_")
                        if brick_name in brick_details:
                            subvolume = brick_details[brick_name]
                            try:
                                NS._int.wclient.delete(
                                    "clusters/%s/Volumes/%s/Bricks/%s/%s" % (
                                        NS.tendrl_context.integration_id,
                                        self.parameters['Volume.vol_id'],
                                        subvolume,
                                        brick_name
                                    ),
                                    recursive=True
                                )
                                # If sub_vol is empty then it will delete
                                NS._int.wclient.delete(
                                "clusters/%s/Volumes/%s/Bricks/%s" % (
                                        NS.tendrl_context.integration_id,
                                        self.parameters['Volume.vol_id'],
                                        subvolume
                                    ),
                                    dir = True
                                )
                            except (etcd.EtcdKeyNotFound, etcd.EtcdDirNotEmpty):
                                continue
            except Exception:
                raise
            finally:
                lock.release()

            Event(
                Message(
                    priority="info",
                    publisher=NS.publisher_id,
                    payload={
                        "message": "Deleted bricks for volume %s from central store" %
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
                        "message": "Volume shrink failed for volume %s" %
                        self.parameters['Volume.volname']
                    },
                    job_id=self.parameters["job_id"],
                    flow_id=self.parameters["flow_id"],
                    cluster_id=NS.tendrl_context.integration_id,
                )
            )
            return False
