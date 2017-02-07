from tendrl.commons.etcdobj import EtcdObj
from tendrl.gluster_integration import objects


class VolumeOptions(objects.GlusterIntegrationBaseObject):
    def __init__(
        self,
        vol_id=None,
        options=None,
    ):
        super(VolumeOptions, self).__init__()

        self.value = 'clusters/%s/Volumes/%s/options'
        self.vol_id = vol_id
        self.options = options
        self._etcd_cls = _VolumeOptions


class _VolumeOptions(EtcdObj):
    """A table of the Volume options, lazily updated
    """
    __name__ = 'clusters/%s/Volumes/%s'
    _tendrl_cls = VolumeOptions

    def render(self):
        self.__name__ = self.__name__ % (
            tendrl_ns.tendrl_context.integration_id,
            self.vol_id
        )
        return super(_VolumeOptions, self).render()
