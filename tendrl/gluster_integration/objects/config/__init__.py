from tendrl.commons.etcdobj import EtcdObj
from tendrl.commons import config as cmn_config


from tendrl.commons import objects

class Config(objects.BaseObject):
    def __init__(self, config=None, *args, **kwargs):
        super(Config, self).__init__(*args, **kwargs)

        self.value = '_tendrl/config/gluster_integration'
        self.data = config or cmn_config.load_config(
            'gluster-integration',
            "/etc/tendrl/gluster-integration/gluster-integration.conf.yaml"
        )
        self._etcd_cls = _ConfigEtcd


class _ConfigEtcd(EtcdObj):
    """Config etcd object, lazily updated
    """
    __name__ = '_tendrl/config/gluster_integration'
    _tendrl_cls = Config
