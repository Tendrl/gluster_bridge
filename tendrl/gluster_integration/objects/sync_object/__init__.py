import logging

from tendrl.commons.etcdobj import EtcdObj
from tendrl.commons import objects

LOG = logging.getLogger(__name__)


class SyncObject(objects.BaseObject):
    def __init__(
        self,
        data=None,
        *args,
        **kwargs
    ):
        super(SyncObject, self).__init__(*args, **kwargs)

        self.value = 'clusters/%s/raw_map'
        self.data = data
        self._etcd_cls = _SyncObject


class _SyncObject(EtcdObj):
    """A table of the _SyncObject, lazily updated
    """
    __name__ = 'clusters/%s/raw_map'
    _tendrl_cls = SyncObject

    def render(self):
        self.__name__ = self.__name__ % (NS.tendrl_context.integration_id)
        return super(_SyncObject, self).render()
