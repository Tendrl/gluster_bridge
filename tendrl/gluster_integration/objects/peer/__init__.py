import logging

from tendrl.commons.etcdobj import EtcdObj
from tendrl.commons import objects

LOG = logging.getLogger(__name__)


class Peer(objects.BaseObject):
    def __init__(
        self,
        state=None,
        hostname=None,
        peer_uuid=None,
        *args,
        **kwargs
    ):
        super(Peer, self).__init__(*args, **kwargs)

        self.value = 'clusters/%s/Peers/%s'
        self.state = state
        self.hostname = hostname
        self.peer_uuid = peer_uuid
        self._etcd_cls = _Peer


class _Peer(EtcdObj):
    """A table of the Peer, lazily updated
    """
    __name__ = 'clusters/%s/Peers/%s'
    _tendrl_cls = Peer

    def render(self):
        self.__name__ = self.__name__ % (NS.tendrl_context.integration_id, self.peer_uuid)
        return super(_Peer, self).render()
