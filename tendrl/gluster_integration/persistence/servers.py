from tendrl.commons.etcdobj.etcdobj import EtcdObj
from tendrl.commons.etcdobj import fields


class Peer(EtcdObj):
    """A table of the peers seen by the pull, lazily updated

    """
    __name__ = 'clusters/%s/Peers/%s'

    cluster_id = fields.StrField("cluster_id")
    state = fields.StrField("state")
    hostname = fields.StrField("hostname")
    peer_uuid = fields.StrField("peer_uuid")
    updated = fields.StrField("updated")

    def render(self):
        self.__name__ = self.__name__ % (self.cluster_id, self.peer_uuid)
        return super(Peer, self).render()


class Volume(EtcdObj):
    """A table of the volumes seen by the pull.

    """
    __name__ = 'clusters/%s/Volumes/%s'

    cluster_id = fields.StrField("cluster_id")
    vol_id = fields.StrField("vol_id")
    vol_type = fields.StrField("vol_type")
    name = fields.StrField("name")
    status = fields.StrField("status")
    brick_count = fields.StrField("brick_count")
    deleted = fields.StrField("deleted")
    transport_type = fields.StrField("transport_type")
    snap_count = fields.IntField("snap_count")
    stripe_count = fields.IntField("stripe_count")
    replica_count = fields.IntField("replica_count")
    subvol_count = fields.IntField("subvol_count")
    arbiter_count = fields.IntField("arbiter_count")
    disperse_count = fields.IntField("disperse_count")
    redundancy_count = fields.IntField("redundancy_count")
    quorum_status = fields.StrField("quorum_status")
    snapd_status = fields.StrField("snapd_status")
    snapd_inited = fields.StrField("snapd_inited")
    rebal_id = fields.StrField("rebal_id")
    rebal_status = fields.StrField("rebal_status")
    rebal_failures = fields.IntField("rebal_failures")
    rebal_skipped = fields.IntField("rebal_skipped")
    rebal_lookedup = fields.IntField("rebal_lookedup")
    rebal_files = fields.IntField("rebal_files")
    rebal_data = fields.StrField("rebal_data")

    def render(self):
        self.__name__ = self.__name__ % (self.cluster_id, self.vol_id)
        return super(Volume, self).render()


class Brick(EtcdObj):
    """A table of the volumes seen by the pull.

    """
    __name__ = 'clusters/%s/Volumes/%s/Bricks/%s'

    cluster_id = fields.StrField("cluster_id")
    vol_id = fields.StrField("vol_id")
    path = fields.StrField("path")
    hostname = fields.StrField("hostname")
    port = fields.StrField("port")
    status = fields.StrField("status")
    filesystem_type = fields.StrField("fs_type")
    mount_opts = fields.StrField("mount_opts")

    def render(self):
        self.__name__ = self.__name__ % (
            self.cluster_id,
            self.vol_id,
            self.path.replace("/", "_")
        )
        return super(Brick, self).render()


class VolumeOptions(EtcdObj):
    """A table of the volume options seen by the pull.

    """
    __name__ = 'clusters/%s/Volumes/%s'

    cluster_id = fields.StrField("cluster_id")
    vol_id = fields.StrField("vol_id")
    Options = fields.DictField("Options", {'str': 'str'})

    def render(self):
        self.__name__ = self.__name__ % (
            self.cluster_id,
            self.vol_id
        )
        return super(VolumeOptions, self).render()
