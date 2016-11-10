from tendrl.bridge_common.etcdobj.etcdobj import EtcdObj
from tendrl.bridge_common.etcdobj import fields


class SDSOperation(EtcdObj):
    """A table for storing SDS operations definitions YAML

    """
    __name__ = 'clusters/%s/definitions'

    cluster_id = fields.StrField("cluster_id")
    # Here the data represents the whole content of SDS definitions YAML file
    # The same is uploaded to central store for easy access while other flows
    data = fields.StrField("data")
    updated = fields.StrField("updated")

    def render(self):
        self.__name__ = self.__name__ % self.cluster_id
        return super(SDSOperation, self).render()
