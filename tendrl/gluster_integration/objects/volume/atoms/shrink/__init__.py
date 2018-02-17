import etcd

from tendrl.commons import objects
from tendrl.commons.utils import log_utils as logger
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
            if vol.replica_count != self.parameters.get(
                    'Volume.replica_count'
            ):
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

        logger.log(
            "info",
            NS.publisher_id,
            {"message": "Shrinking the volume %s" %
             self.parameters['Volume.volname']},
            job_id=self.parameters["job_id"],
            flow_id=self.parameters["flow_id"],
            integration_id=NS.tendrl_context.integration_id
        )
        if NS.gdeploy_plugin.shrink_volume(
                self.parameters.get('Volume.volname'),
                self.parameters.get('Volume.bricks'),
                action,
                **args
        ):
            logger.log(
                "info",
                NS.publisher_id,
                {"message": "Shrinked the volume %s" %
                 self.parameters['Volume.volname']},
                job_id=self.parameters["job_id"],
                flow_id=self.parameters["flow_id"],
                integration_id=NS.tendrl_context.integration_id
            )
            if action != "commit" and not "decrease_"\
               "replica_count" in args:
                return True
            try:
                # Delete the bricks from central store
                # Acquire lock before deleting the bricks from etcd
                # We are blocking till we acquire the lock
                # the lock will live for 60 sec after which it will
                # be released.
                lock = etcd.Lock(NS._int.wclient, 'volume')

                while not lock.is_acquired:
                    try:
                        # with ttl set, lock will be blocked only for 60 sec
                        # after which it will raise lock_expired exception.
                        # if this is raised, we have to retry for lock
                        lock.acquire(blocking=True, lock_ttl=60)
                        if lock.is_acquired:
                            # renewing lock as we are not sure, how long we
                            # were blocked before the lock was given.
                            # NOTE: blocked time also counts as ttl
                            lock.acquire(lock_ttl=60)
                    except etcd.EtcdLockExpired:
                        continue
                for sub_vol in self.parameters.get('Volume.bricks'):
                    for b in sub_vol:
                        brick_name = b.keys()[0] + ":" + b.values()[0].replace(
                            "/", "_"
                        )
                        try:
                            NS._int.wclient.delete(
                                "clusters/%s/Volumes/%s/Bricks/%s" % (
                                    NS.tendrl_context.integration_id,
                                    self.parameters['Volume.vol_id'],
                                    brick_name
                                ),
                                recursive=True
                            )
                        except etcd.EtcdKeyNotFound:
                            continue
            except Exception:
                raise
            finally:
                lock.release()

            logger.log(
                "info",
                NS.publisher_id,
                {"message": "Deleted bricks for volume %s"
                 " from central store" % self.parameters[
                     'Volume.volname'
                 ]},
                job_id=self.parameters["job_id"],
                flow_id=self.parameters["flow_id"],
                integration_id=NS.tendrl_context.integration_id
            )
            return True
        else:
            logger.log(
                "error",
                NS.publisher_id,
                {"message": "Volume shrink failed for volume %s" %
                 self.parameters['Volume.volname']},
                job_id=self.parameters["job_id"],
                flow_id=self.parameters["flow_id"],
                integration_id=NS.tendrl_context.integration_id
            )
            return False
