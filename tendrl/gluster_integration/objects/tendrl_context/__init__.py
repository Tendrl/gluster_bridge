import json
import os
import subprocess

from tendrl.commons.event import Event
from tendrl.commons.message import Message

from tendrl.commons.etcdobj import EtcdObj
from tendrl.gluster_integration import objects


class TendrlContext(objects.GlusterIntegrationBaseObject):
    def __init__(self, integration_id=None, *args, **kwargs):
        super(TendrlContext, self).__init__(*args, **kwargs)

        self.value = 'clusters/%s/TendrlContext'

        # integration_id is the Tendrl generated cluster UUID
        self.integration_id = integration_id or self._get_local_integration_id()
        self.sds_name, self.sds_version = self._get_sds_details()
        self.integration_name = self.sds_name + "_" + self.integration_id
        self._etcd_cls = _TendrlContextEtcd

    def create_local_integration_id(self):
        tendrl_context_path = "/etc/tendrl/gluster-integration/integration_id"
        with open(tendrl_context_path, 'wb+') as f:
            f.write(self.integration_id)
            Event(
                Message(
                    Message.priorities.INFO,
                    Message.publishers.GLUSTER_INTEGRATION,
                    {"message": "SET_LOCAL: tendrl_ns.gluster_integration."
                                "objects.TendrlContext.integration_id==%s" %
                                self.integration_id
                     }
                )
            )

    def _get_local_integration_id(self):
        try:
            tendrl_context_path = "/etc/tendrl/gluster-integration/integration_id"
            if os.path.isfile(tendrl_context_path):
                with open(tendrl_context_path) as f:
                    integration_id = f.read()
                    if integration_id:
                        Event(
                            Message(
                                Message.priorities.INFO,
                                Message.publishers.GLUSTER_INTEGRATION,
                                {"message": "GET_LOCAL: tendrl_ns."
                                            "gluster_integration.objects."
                                            "TendrlContext.integration_id==%s"
                                            %integration_id
                                 }
                            )
                        )
                        return integration_id
        except AttributeError:
            return None

    def _get_sds_details(self):
        # get the gluster version details
        cmd = subprocess.Popen(
            "gluster --version",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        out, err = cmd.communicate()
        if err and 'command not found' in err:
            Event(
                Message(
                    Message.priorities.INFO,
                    Message.publishers.GLUSTER_INTEGRATION,
                    {"message": "gluster not installed on host"}
                )
            )
            return None
        lines = out.split('\n')
        version = lines[0].split()[1]
        name = lines[0].split()[0]

        return name, version

class _TendrlContextEtcd(EtcdObj):
    """A table of the node context, lazily updated
    """
    __name__ = 'clusters/%s/TendrlContext'
    _tendrl_cls = TendrlContext

    def render(self):
        self.__name__ = self.__name__ % self.integration_id
        return super(_TendrlContextEtcd, self).render()
