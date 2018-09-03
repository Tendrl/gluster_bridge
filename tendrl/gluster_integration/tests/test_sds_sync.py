import etcd
import time
import maps
import subprocess
import importlib
from mock import MagicMock
from mock import patch

from tendrl.commons.objects import BaseObject
from tendrl.commons.utils import etcd_utils
from tendrl.commons.utils import event_utils

from tendrl.commons.objects import node_context as node

from tendrl.gluster_integration.tests.test_init import init

@patch.object(BaseObject, "save")
@patch.object(BaseObject, "load_all")
@patch.object(event_utils, "emit_event")
@patch.object(BaseObject, "hash_compare_with_central_store")
@patch.object(etcd_utils, "refresh")
def test_brick_status_alert(
    refresh, compare, emit_event, load_all, save
):
    compare.return_value = True
    refresh.return_value = True
    save.return_value = True
    obj = NS.tendrl.objects.GlusterBrick(
        integration_id="77deef29-b8e5-4dc5-8247-21e2a409a66a",
        fqdn="dhcp12-12.lab.abc.com",
        hostname="dhcp12-12.lab.abc.com",
        status="started",
        vol_name="v1",
        brick_path="/gluster/b1",
        node_id="3c4b48cc-1a61-4c64-90d6-eba840c00081"
    )
    load_all.return_value = [obj]
    sds_sync = importlib.import_module(
        'tendrl.gluster_integration.sds_sync'
    )
    sds_sync.Event = MagicMock()
    sds_sync.ExceptionMessage = MagicMock()
    with patch.object(
        etcd.Lock, 'acquire'
    ) as mock_acquire:
        mock_acquire.return_value = True
        with patch.object(
            etcd.Lock, 'is_acquired'
        ) as mock_is_acq:
            mock_is_acq.return_value = True
            with patch.object(
                etcd.Lock, 'release'
            ) as mock_rel:
                mock_rel.return_value = True
                sds_sync.brick_status_alert(
                    "dhcp12-12.lab.abc.com"
                )
                emit_event.assert_called_with(
                    'brick_status',
                    'Stopped',
                    'Brick:/gluster/b1 in '
                    'volume:v1 '
                    'has Stopped',
                    'volume_v1|brick_/gluster/b1',
                    'WARNING',
                    tags={'node_id': '3c4b48cc-1a61-4c64-90d6-eba840c00081',
                          'volume_name': 'v1',
                          'fqdn': 'dhcp12-12.lab.abc.com',
                          'entity_type': 'brick'
                          }
                )
    sds_sync.brick_status_alert(
        "dhcp12-12.lab.abc.com"
    )


@patch('tendrl.gluster_integration.' +
       'sds_sync.GlusterIntegrationSdsSyncStateThread.run')
def test_sds_sync_run_called(mock_run):
    mock_run.return_value = None
    sds_sync = importlib.import_module(
        'tendrl.gluster_integration.sds_sync'
    )
    sds_sync.GlusterIntegrationSdsSyncStateThread().start()
    mock_run.assert_called_once()


@patch.object(BaseObject, "load")
@patch.object(subprocess, 'call')
@patch.object(time, 'sleep')
def test_sds_sync_run(time_sleep, subproc_call, load):
    time_sleep.return_value = None
    subproc_call.return_value = 0
    init()
    obj = NS.tendrl.objects.GlusterBrick(
        integration_id="77deef29-b8e5-4dc5-8247-21e2a409a66a",
        fqdn="dhcp12-12.lab.abc.com",
        hostname="dhcp12-12.lab.abc.com",
        status="started",
        vol_name="v1",
        brick_path="/gluster/b1",
        node_id="3c4b48cc-1a61-4c64-90d6-eba840c00081"
    )
    # reasoning of passing 'obj'
    # load_all was providing list of objects, like [obj] (line# 37 in this file)
    # so load should return a single value
    load.retun_value = obj

    def dummy_callable():
        pass
    setattr(NS, "tendrl_context", maps.NamedDict())
    NS.tendrl_context["integration_id"] = "int-id"
    NS.tendrl_context["cluster_name"] = "cluster1"
    NS.tendrl_context["sds_name"] = "gluster"
    # in below line, passing a callable function to 'load' key,
    # as the tox was complaining about the value being not callable
    # assigning a MagicMock() object, making it indefinitely long
    NS.tendrl_context["load"] = dummy_callable
    NS.publisher_id = "gluster-integration"
    sds_sync = importlib.import_module(
        'tendrl.gluster_integration.sds_sync'
    )
    with patch.object(etcd_utils, "read") as utils_read:
        # not sure why the below mock not working
        utils_read.return_value = maps.NamedDict(
            value='{"tags":[]}'
        )
        sds_sync.GlusterIntegrationSdsSyncStateThread().run()
    # below is a dummy assert, which should be replaced by
    # other assertions to get the code coverage
    # and check / mock some exceptions
    subproc_call.assert_called_once()
