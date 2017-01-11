from tendrl.commons.etcdobj.etcdobj import EtcdObj
from tendrl.commons.etcdobj import fields


class SyncObject(EtcdObj):
    """A table for storing a FIFO of ClusterMonitor 'sync objects', i.e.

    cluster maps.

    """
    __name__ = 'clusters/%s/raw_map'

    cluster_id = fields.StrField("cluster_id")
    data = fields.StrField("data")
    updated = fields.StrField("updated")

    def render(self):
        self.__name__ = self.__name__ % self.cluster_id
        return super(SyncObject, self).render()
