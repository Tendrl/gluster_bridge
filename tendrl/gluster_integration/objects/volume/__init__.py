import logging

from tendrl.commons.etcdobj import EtcdObj
from tendrl.commons import objects

LOG = logging.getLogger(__name__)


class Volume(objects.BaseObject):
    def __init__(
        self,
        vol_id=None,
        vol_type=None,
        name=None,
        status=None,
        brick_count=None,
        deleted=None,
        transport_type=None,
        snap_count=None,
        stripe_count=None,
        replica_count=None,
        subvol_count=None,
        arbiter_count=None,
        disperse_count=None,
        redundancy_count=None,
        quorum_status=None,
        snapd_status=None,
        snapd_inited=None,
        rebal_id=None,
        rebal_status=None,
        rebal_failures=None,
        rebal_skipped=None,
        rebal_lookedup=None,
        rebal_files=None,
        rebal_data=None,
        usable_capacity=None,
        used_capacity=None,
        pcnt_used=None,
        *args,
        **kwargs
    ):
        super(Volume, self).__init__(*args, **kwargs)

        self.vol_id = vol_id
        self.vol_type = vol_type
        self.name = name
        self.status = status
        self.brick_count = brick_count
        self.deleted = deleted
        self.transport_type = transport_type
        self.snap_count = snap_count
        self.stripe_count = stripe_count
        self.replica_count = replica_count
        self.subvol_count = subvol_count
        self.arbiter_count = arbiter_count
        self.disperse_count = disperse_count
        self.redundancy_count = redundancy_count
        self.quorum_status = quorum_status
        self.snapd_status = snapd_status
        self.snapd_inited = snapd_inited
        self.rebal_id = rebal_id
        self.rebal_status = rebal_status
        self.rebal_failures = rebal_failures
        self.rebal_skipped = rebal_skipped
        self.rebal_lookedup = rebal_lookedup
        self.rebal_files = rebal_files
        self.rebal_data = rebal_data
        self.usable_capacity = usable_capacity
        self.used_capacity = used_capacity
        self.pcnt_used = pcnt_used
        self._etcd_cls = _Volume


class _Volume(EtcdObj):
    """A table of the Peer, lazily updated
    """
    __name__ = 'clusters/%s/Volumes/%s'
    _tendrl_cls = Volume

    def render(self):
        self.__name__ = self.__name__ % (NS.tendrl_context.integration_id, self.vol_id)
        return super(_Volume, self).render()
