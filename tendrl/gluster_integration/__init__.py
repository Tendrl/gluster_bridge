# flake8: noqa
try:
    from gevent import monkey
except ImportError:
    pass
else:
    monkey.patch_all()

from tendrl.commons import TendrlNS


class GlusterIntegrationNS(TendrlNS):
    def __init__(self):
        self,
        ns_name = "tendrl.gluster_integration"
        ns_src = "tendrl.gluster_integration"
        super(GlusterIntegrationNS, self).__init__(ns_name, ns_src)
