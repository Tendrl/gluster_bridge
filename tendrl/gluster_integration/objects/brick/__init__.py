from tendrl.commons.etcdobj import EtcdObj
from tendrl.gluster_integration import objects


class Brick(objects.GlusterIntegrationBaseObject):
    def __init__(
        self,
        vol_id=None,
        path=None,
        hostname=None,
        port=None,
        status=None,
        filesystem_type=None,
        mount_opts=None,
        *args,
        **kwargs
    ):
        super(Brick, self).__init__(*args, **kwargs)

        self.value = 'clusters/%s/Volumes/%s/Bricks/%s'
        self.vol_id = vol_id
        self.path = path
        self.hostname = hostname
        self.port = port
        self.status = status
        self.filesystem_type = filesystem_type
        self.mount_opts = mount_opts
        self._etcd_cls = _Brick


class _Brick(EtcdObj):
    """A table of the Volume, lazily updated
    """
    __name__ = 'clusters/%s/Volumes/%s/Bricks/%s'
    _tendrl_cls = Brick

    def render(self):
        self.__name__ = self.__name__ % (
            tendrl_ns.tendrl_context.integration_id,
            self.vol_id,
            self.path.replace("/", "_")
        )
        return super(_Brick, self).render()
