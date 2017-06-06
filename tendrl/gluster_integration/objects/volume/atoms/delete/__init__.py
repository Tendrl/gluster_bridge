import etcd
import subprocess

from tendrl.commons.event import Event
from tendrl.commons.message import Message
from tendrl.commons import objects
from tendrl.gluster_integration.objects.volume import Volume


class Delete(objects.BaseAtom):
    def __init__(self, *args, **kwargs):
        super(Delete, self).__init__(*args, **kwargs)

    def run(self):
        vol_id = self.parameters['Volume.vol_id']
        if NS.gdeploy_plugin.stop_volume(
                self.parameters.get('Volume.volname')
        ):
            Event(
                Message(
                    priority="info",
                    publisher=NS.publisher_id,
                    payload={
                        "message": "Stopped the volume %s before delete" %
                        self.parameters['Volume.volname']
                    },
                    job_id=self.parameters["job_id"],
                    flow_id=self.parameters["flow_id"],
                    cluster_id=NS.tendrl_context.integration_id,
                )
            )
        else:
            Event(
                Message(
                    priority="error",
                    publisher=NS.publisher_id,
                    payload={
                        "message": "Could not stop volume %s before delete" %
                        self.parameters['Volume.volname']
                    },
                    job_id=self.parameters["job_id"],
                    flow_id=self.parameters["flow_id"],
                    cluster_id=NS.tendrl_context.integration_id,
                )
            )
            return False
        args = {}
        if self.parameters.get('Volume.volname') is not None:
            args.update({
                "format_bricks": self.parameters.get('Volume.format_bricks')
            })

        if NS.gdeploy_plugin.delete_volume(
                self.parameters.get('Volume.volname'),
                **args
        ):
            Event(
                Message(
                    priority="info",
                    publisher=NS.publisher_id,
                    payload={
                        "message": "Deleted the volume %s" %
                        self.parameters['Volume.volname']
                    },
                    job_id=self.parameters["job_id"],
                    flow_id=self.parameters["flow_id"],
                    cluster_id=NS.tendrl_context.integration_id,
                )
            )
        else:
            Event(
                Message(
                    priority="error",
                    publisher=NS.publisher_id,
                    payload={
                        "message": "Failed to delete volume %s" %
                        self.parameters['Volume.volname']
                    },
                    job_id=self.parameters["job_id"],
                    flow_id=self.parameters["flow_id"],
                    cluster_id=NS.tendrl_context.integration_id,
                )
            )
            return False

        while True:
            try:
                # Acquire lock before deleting the volume from etcd
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
                
                NS._int.wclient.delete(
                    "clusters/%s/Volumes/%s" % (
                        NS.tendrl_context.integration_id,
                        self.parameters['Volume.vol_id']
                    ),
                    recursive=True
                )
            except (etcd.EtcdKeyNotFound, KeyError):
                Event(
                    Message(
                        priority="info",
                        publisher=NS.publisher_id,
                        payload={
                            "message": "Deleted the volume %s" %
                            self.parameters['Volume.volname']
                        },
                        job_id=self.parameters["job_id"],
                        flow_id=self.parameters["flow_id"],
                        cluster_id=NS.tendrl_context.integration_id,
                    )
                )
            finally:
                lock.release()

                return True
