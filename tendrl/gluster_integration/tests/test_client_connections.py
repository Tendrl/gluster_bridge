import etcd
import importlib
import maps
from mock import patch

from tendrl.commons.objects import BaseObject
from tendrl.commons.utils import etcd_utils
from tendrl.gluster_integration.tests.test_init import init


def read(param):
    if "subvolume1" in param:
        raise etcd.EtcdKeyNotFound
    else:
        keys = maps.NamedDict(
            key="sub_vol1/dhcp123-12.lab.abc.com:_gluster_b1"
        )
        return maps.NamedDict(leaves=[keys])


@patch.object(BaseObject, "load")
@patch.object(BaseObject, "save")
def test_sync_volume_connections(save, load):
    init()
    client_connections = importlib.import_module(
        'tendrl.gluster_integration.sds_sync.client_connections'
    )
    init()
    obj = NS.tendrl.objects.GlusterBrick(
        "dhcp123-12.lab.abc.com",
        "_gluster_b1"
    )
    obj.client_count = 1
    load.return_value = obj
    with patch.object(etcd_utils, "read", read):
        client_connections.sync_volume_connections(
            [NS.gluster.objects.Volume()]
        )
        save.assert_called()
