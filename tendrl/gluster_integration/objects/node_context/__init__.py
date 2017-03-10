import logging
import os
import socket
import uuid

from tendrl.commons.etcdobj import EtcdObj
from tendrl.commons.utils import cmd_utils

from tendrl.commons import objects


LOG = logging.getLogger(__name__)


class NodeContext(objects.BaseObject):

    def __init__(self, machine_id=None, node_id=None, fqdn=None,
                 tags=None, status=None, *args, **kwargs):
        super(NodeContext, self).__init__(*args, **kwargs)

        self.value = 'clusters/%s/nodes/%s/NodeContext'
        self.machine_id = machine_id or self._get_machine_id()
        self.node_id = node_id or self._get_node_id() or self._create_node_id()
        self.fqdn = fqdn or socket.getfqdn()
        self.tags = tags or ""
        self.status = status or "UP"
        self._etcd_cls = _NodeContextEtcd

    def _get_machine_id(self):
        cmd = cmd_utils.Command("cat /etc/machine-id")
        out, err, rc = cmd.run()
        return str(out)

    def _create_node_id(self, node_id=None):
        node_id = node_id or str(uuid.uuid4())
        local_node_context = "/etc/tendrl/node-agent/NodeContext"
        with open(local_node_context, 'wb+') as f:
            f.write(node_id)
            LOG.info("SET_LOCAL: "
                     "NS.gluster_integration.objects.NodeContext.node_id==%s" %
                     node_id)
        return node_id

    def _get_node_id(self):
        try:
            local_node_context = "/etc/tendrl/node-agent/NodeContext"
            if os.path.isfile(local_node_context):
                with open(local_node_context) as f:
                    node_id = f.read()
                    if node_id:
                        LOG.info(
                            "GET_LOCAL: NS.gluster_integration.objects.NodeContext"
                            ".node_id==%s" % node_id)
                        return node_id
        except AttributeError:
            return None


class _NodeContextEtcd(EtcdObj):
    """A table of the node context, lazily updated

    """
    __name__ = 'clusters/%s/nodes/%s/NodeContext'
    _tendrl_cls = NodeContext

    def render(self):
        self.__name__ = self.__name__ % (
            NS.tendrl_context.integration_id,
            NS.node_context.node_id
        )
        return super(_NodeContextEtcd, self).render()
