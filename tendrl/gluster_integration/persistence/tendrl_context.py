from tendrl.commons.etcdobj.etcdobj import EtcdObj
from tendrl.commons.etcdobj import fields


class TendrlContext(EtcdObj):
    """A table of the tendrl context, lazily updated

    """
    __name__ = 'clusters/%s/Tendrl_context'

    sds_version = fields.StrField("sds_version")
    sds_name = fields.StrField("sds_name")
    cluster_id = fields.StrField("cluster_id")

    def render(self):
        self.__name__ = self.__name__ % self.cluster_id
        return super(TendrlContext, self).render()
