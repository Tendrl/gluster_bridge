import json
import maps
from mock import MagicMock
from mock import patch
import os
import socket
import subprocess
import time

from tendrl.commons.objects import BaseObject
from tendrl.commons.utils import etcd_utils
from tendrl.commons.utils import monitoring_utils
from tendrl.gluster_integration import ini2json
from tendrl.gluster_integration.message.callback import Callback
from tendrl.gluster_integration.tests.test_init import init


@patch.object(BaseObject, "load")
def test_quorum_lost(load):
    init()
    load.return_value = maps.NamedDict(
        short_name="77deef29-b8e5-4dc5-8247-21e2a409a66a"
    )
    event = {"message": {"volume": "v1"},
             "event": "QUORUM_LOST",
             "ts": 1486634392,
             "nodeid": "0f5b4b99-fa4a-4b8f-be52-770b42879d67"
             }
    NS.gluster.objects.NativeEvents = MagicMock()
    Callback().quorum_lost(event)
    NS.gluster.objects.NativeEvents.assert_called_with(
        'quorum|v1',
        current_value='quorum_lost',
        message='Quorum of volume: v1 is lost in cluster '
                '77deef29-b8e5-4dc5-8247-21e2a409a66a',
        severity='warning',
        tags={'volume_name': 'v1',
              'entity_type': 'volume'
              }
    )


@patch.object(BaseObject, "load")
def test_quorum_regained(load):
    init()
    load.return_value = maps.NamedDict(
        short_name="77deef29-b8e5-4dc5-8247-21e2a409a66a"
    )
    event = {"message": {"volume": "v1"},
             "event": "QUORUM_REGAINED",
             "ts": 1486634392,
             "nodeid": "0f5b4b99-fa4a-4b8f-be52-770b42879d67"
             }
    NS.gluster.objects.NativeEvents = MagicMock()
    Callback().quorum_regained(event)
    NS.gluster.objects.NativeEvents.assert_called_with(
        'quorum|v1',
        current_value='quorum_gained',
        message='Quorum of volume: v1 is regained in cluster '
                '77deef29-b8e5-4dc5-8247-21e2a409a66a',
        severity='recovery',
        tags={'volume_name': 'v1',
              'entity_type': 'volume'
              }
    )


@patch.object(BaseObject, "load")
def test_svc_connected(load):
    init()
    NS.node_context.fqdn = "node-test"
    load.return_value = maps.NamedDict(
        short_name="77deef29-b8e5-4dc5-8247-21e2a409a66a"
    )
    event = {"message": {"volume": "v1", "svc_name": "test"},
             "event": "SVC_CONNECTED",
             "ts": 1486634392,
             "nodeid": "0f5b4b99-fa4a-4b8f-be52-770b42879d67"
             }
    NS.gluster.objects.NativeEvents = MagicMock()
    Callback().svc_connected(event)
    NS.gluster.objects.NativeEvents.assert_called_with(
        'svc_connection|test_node-testv1',
        current_value='service_connected',
        message='Service: test is connected on node node-test '
                'of cluster 77deef29-b8e5-4dc5-8247-21e2a409a66a',
        severity='recovery'
    )


@patch.object(BaseObject, "load")
def test_svc_disconnected(load):
    init()
    NS.node_context.fqdn = "node-test"
    load.return_value = maps.NamedDict(
        short_name="77deef29-b8e5-4dc5-8247-21e2a409a66a"
    )
    event = {"message": {"volume": "v1", "svc_name": "test"},
             "event": "SVC_DISCONNECTED",
             "ts": 1486634392,
             "nodeid": "0f5b4b99-fa4a-4b8f-be52-770b42879d67"
             }
    NS.gluster.objects.NativeEvents = MagicMock()
    Callback().svc_disconnected(event)
    NS.gluster.objects.NativeEvents.assert_called_with(
        'svc_connection|test_node-testv1',
        current_value='service_disconnected',
        message='Service: test is disconnected on node node-test '
                'of cluster 77deef29-b8e5-4dc5-8247-21e2a409a66a',
        severity='warning'
    )


@patch.object(BaseObject, "load")
def test_ec_min_bricks_not_up(load):
    init()
    load.return_value = maps.NamedDict(
        short_name="77deef29-b8e5-4dc5-8247-21e2a409a66a"
    )
    event = {"message": {"subvol": "sub_vol"},
             "event": "EC_MIN_BRICKS_NOT_UP",
             "ts": 1486634392,
             "nodeid": "0f5b4b99-fa4a-4b8f-be52-770b42879d67"
             }
    NS.gluster.objects.NativeEvents = MagicMock()
    Callback().ec_min_bricks_not_up(event)
    NS.gluster.objects.NativeEvents.assert_called_with(
        'ec_min_bricks_up|sub_vol',
        current_value='ec_min_bricks_not_up',
        message='Minimum number of bricks not up in EC subvolume: sub_vol '
                'in cluster 77deef29-b8e5-4dc5-8247-21e2a409a66a',
        severity='warning',
        tags={'entity_type': 'volume', 'volume_name': 'sub_vol'}
    )


@patch.object(BaseObject, "load")
def test_ec_min_bricks_up(load):
    init()
    load.return_value = maps.NamedDict(
        short_name="77deef29-b8e5-4dc5-8247-21e2a409a66a"
    )
    event = {"message": {"subvol": "sub_vol"},
             "event": "EC_MIN_BRICKS_UP",
             "ts": 1486634392,
             "nodeid": "0f5b4b99-fa4a-4b8f-be52-770b42879d67"
             }
    NS.gluster.objects.NativeEvents = MagicMock()
    Callback().ec_min_bricks_up(event)
    NS.gluster.objects.NativeEvents.assert_called_with(
        'ec_min_bricks_up|sub_vol',
        current_value='ec_min_bricks_up',
        message='Minimum number of bricks back online in EC subvolume: '
                'sub_vol in cluster 77deef29-b8e5-4dc5-8247-21e2a409a66a',
        severity='recovery',
        tags={'entity_type': 'volume', 'volume_name': 'sub_vol'}
    )


@patch.object(BaseObject, "load")
def test_afr_quorum_met(load):
    init()
    load.return_value = maps.NamedDict(
        short_name="77deef29-b8e5-4dc5-8247-21e2a409a66a"
    )
    event = {"message": {"subvol": "sub_vol"},
             "event": "AFR_QUORUM_MET",
             "ts": 1486634392,
             "nodeid": "0f5b4b99-fa4a-4b8f-be52-770b42879d67"
             }
    NS.gluster.objects.NativeEvents = MagicMock()
    Callback().afr_quorum_met(event)
    NS.gluster.objects.NativeEvents.assert_called_with(
        'afr_quorum_state|sub_vol',
        current_value='afr_quorum_met',
        message='Afr quorum is met for subvolume: sub_vol in cluster '
                '77deef29-b8e5-4dc5-8247-21e2a409a66a',
        severity='recovery',
        tags={'volume_name': 'sub_vol', 'entity_type': 'volume'}
    )


@patch.object(BaseObject, "load")
def test_afr_quorum_fail(load):
    init()
    load.return_value = maps.NamedDict(
        short_name="77deef29-b8e5-4dc5-8247-21e2a409a66a"
    )
    event = {"message": {"subvol": "sub_vol"},
             "event": "AFR_QUORUM_FAIL",
             "ts": 1486634392,
             "nodeid": "0f5b4b99-fa4a-4b8f-be52-770b42879d67"
             }
    NS.gluster.objects.NativeEvents = MagicMock()
    Callback().afr_quorum_fail(event)
    NS.gluster.objects.NativeEvents.assert_called_with(
        'afr_quorum_state|sub_vol',
        current_value='afr_quorum_failed',
        message='Afr quorum has failed for subvolume: sub_vol in cluster '
                '77deef29-b8e5-4dc5-8247-21e2a409a66a',
        severity='warning',
        tags={'entity_type': 'volume', 'volume_name': 'sub_vol'}
    )


@patch.object(BaseObject, "load")
def test_afr_subvol_up(load):
    init()
    load.return_value = maps.NamedDict(
        short_name="77deef29-b8e5-4dc5-8247-21e2a409a66a"
    )
    event = {"message": {"subvol": "sub_vol"},
             "event": "AFR_SUBVOL_UP",
             "ts": 1486634392,
             "nodeid": "0f5b4b99-fa4a-4b8f-be52-770b42879d67"
             }
    NS.gluster.objects.NativeEvents = MagicMock()
    Callback().afr_subvol_up(event)
    NS.gluster.objects.NativeEvents.assert_called_with(
        'afr_subvol_state|sub_vol',
        current_value='afr_subvol_up',
        message='Afr subvolume: sub_vol is back up in cluster '
                '77deef29-b8e5-4dc5-8247-21e2a409a66a',
        severity='recovery',
        tags={'entity_type': 'volume', 'volume_name': 'sub_vol'}
    )


@patch.object(BaseObject, "load")
def test_afr_subvols_down(load):
    init()
    load.return_value = maps.NamedDict(
        short_name="77deef29-b8e5-4dc5-8247-21e2a409a66a"
    )
    event = {"message": {"subvol": "volume1-replica-2"},
             "event": "AFR_SUBVOLS_DOWN",
             "ts": 1486634392,
             "nodeid": "0f5b4b99-fa4a-4b8f-be52-770b42879d67"
             }
    NS.gluster.objects.NativeEvents = MagicMock()
    Callback().afr_subvols_down(event)
    NS.gluster.objects.NativeEvents.assert_called_with(
        'afr_subvol_state|volume1-replica-2',
        current_value='afr_subvol_down',
        message='Afr subvolume: volume1-replica-2 is down in cluster '
                '77deef29-b8e5-4dc5-8247-21e2a409a66a',
        severity='warning',
        tags={'volume_name': 'volume1', 'entity_type': 'volume'}
    )


@patch.object(BaseObject, "load")
def test_unknown_peer(load):
    init()
    load.return_value = maps.NamedDict(
        short_name="77deef29-b8e5-4dc5-8247-21e2a409a66a"
    )
    event = {"message": {"peer": "dhcp123-12.lab.abc.com"},
             "event": "UNKNOWN_PEER",
             "ts": 1486634392,
             "nodeid": "0f5b4b99-fa4a-4b8f-be52-770b42879d67"
             }
    NS.gluster.objects.NativeEvents = MagicMock()
    Callback().unknown_peer(event)
    NS.gluster.objects.NativeEvents.assert_called_with(
        'unknown_peer|dhcp123-12.lab.abc.com',
        alert_notify=True,
        current_value='unknown_peer',
        message='Peer dhcp123-12.lab.abc.com has moved to '
                'unknown state in cluster '
                '77deef29-b8e5-4dc5-8247-21e2a409a66a',
        severity='warning'
    )


@patch.object(BaseObject, "load")
def test_brickpath_resolve_failed(load):
    init()
    load.return_value = maps.NamedDict(
        short_name="77deef29-b8e5-4dc5-8247-21e2a409a66a"
    )
    event = {"message": {"peer": "dhcp123-12.lab.abc.com",
                         "volume": "v1",
                         "brick": "b1"
                         },
             "event": "BRICKPATH_RESOLVE_FAILED",
             "ts": 1486634392,
             "nodeid": "0f5b4b99-fa4a-4b8f-be52-770b42879d67"
             }
    NS.gluster.objects.NativeEvents = MagicMock()
    Callback().brickpath_resolve_failed(event)
    NS.gluster.objects.NativeEvents.assert_called_with(
        'brickpath_resolve_failed|dhcp123-12.lab.abc.comv1b1',
        alert_notify=True,
        current_value='brick_path_resolve_failed',
        message='Brick path resolution failed for brick: b1 . '
                'Volume: v1.Peer: dhcp123-12.lab.abc.com in cluster '
                '77deef29-b8e5-4dc5-8247-21e2a409a66a',
        severity='warning'
    )


@patch.object(BaseObject, "load")
def test_quota_crossed_soft_limit(load):
    init()
    load.return_value = maps.NamedDict(
        short_name="77deef29-b8e5-4dc5-8247-21e2a409a66a"
    )
    event = {"message": {"path": "/gluster/b1",
                         "volume": "v1",
                         "usage": "70%"
                         },
             "event": "QUOTA_CROSSED_SOFT_LIMIT",
             "ts": 1486634392,
             "nodeid": "0f5b4b99-fa4a-4b8f-be52-770b42879d67"
             }
    NS.gluster.objects.NativeEvents = MagicMock()
    Callback().quota_crossed_soft_limit(event)
    NS.gluster.objects.NativeEvents.assert_called_with(
        'quota_crossed_soft_limit|v1/gluster/b1',
        alert_notify=True,
        current_value='quota_crossed_soft_limit',
        message='Quota soft limit crossed in volume: v1 for path: '
                '/gluster/b1. Current usage: 70% in cluster '
                '77deef29-b8e5-4dc5-8247-21e2a409a66a',
        severity='warning'
    )


@patch.object(BaseObject, "load")
def test_bitrot_bad_file(load):
    init()
    load.return_value = maps.NamedDict(
        short_name="77deef29-b8e5-4dc5-8247-21e2a409a66a"
    )
    event = {"message": {"path": "/gluster/b1",
                         "brick": "b1",
                         "gfid": "1"
                         },
             "event": "BITROT_BAD_FILE",
             "ts": 1486634392,
             "nodeid": "0f5b4b99-fa4a-4b8f-be52-770b42879d67"
             }
    NS.gluster.objects.NativeEvents = MagicMock()
    Callback().bitrot_bad_file(event)
    NS.gluster.objects.NativeEvents.assert_called_with(
        'bitrot_bad_file|b1/gluster/b11',
        alert_notify=True,
        current_value='bitrot_bad_file',
        message='File with gfid: 1 is corrupted due to bitrot.  '
                'Brick: b1. Path: /gluster/b1 in cluster '
                '77deef29-b8e5-4dc5-8247-21e2a409a66a',
        severity='warning'
    )


@patch.object(BaseObject, "load")
def test_afr_split_brain(load):
    init()
    load.return_value = maps.NamedDict(
        short_name="77deef29-b8e5-4dc5-8247-21e2a409a66a"
    )
    event = {"message": {"subvol": "volume1-replica-2"},
             "event": "AFR_SPLIT_BRAIN",
             "ts": 1486634392,
             "nodeid": "0f5b4b99-fa4a-4b8f-be52-770b42879d67"
             }
    NS.gluster.objects.NativeEvents = MagicMock()
    Callback().afr_split_brain(event)
    NS.gluster.objects.NativeEvents.assert_called_with(
        'afr_split_brain|volume1-replica-2',
        alert_notify=True,
        current_value='afr_split_brain',
        message='Subvolume: volume1-replica-2 is affected by split-brain.'
                ' Some of thereplicated files in the volume might '
                'be divergent in cluster '
                '77deef29-b8e5-4dc5-8247-21e2a409a66a',
        severity='warning'
    )


@patch.object(BaseObject, "load")
def test_snapshot_soft_limit_reached(load):
    init()
    load.return_value = maps.NamedDict(
        short_name="77deef29-b8e5-4dc5-8247-21e2a409a66a"
    )
    event = {"message": {"volume_name": "volume1"},
             "event": "SNAPSHOT_SOFT_LIMIT_REACHED",
             "ts": 1486634392,
             "nodeid": "0f5b4b99-fa4a-4b8f-be52-770b42879d67"
             }
    NS.gluster.objects.NativeEvents = MagicMock()
    Callback().snapshot_soft_limit_reached(event)
    NS.gluster.objects.NativeEvents.assert_called_with(
        'snapshot_soft_limit_reached|volume1',
        alert_notify=True,
        current_value='snapshot_soft_limit_reached',
        message='Snapshot soft limit reached for volume: '
                'volume1 in cluster '
                '77deef29-b8e5-4dc5-8247-21e2a409a66a',
        severity='warning'
    )


@patch.object(BaseObject, "load")
def test_snapshot_hard_limit_reached(load):
    init()
    load.return_value = maps.NamedDict(
        short_name="77deef29-b8e5-4dc5-8247-21e2a409a66a"
    )
    event = {"message": {"volume_name": "volume1"},
             "event": "SNAPSHOT_HARD_LIMIT_REACHED",
             "ts": 1486634392,
             "nodeid": "0f5b4b99-fa4a-4b8f-be52-770b42879d67"
             }
    NS.gluster.objects.NativeEvents = MagicMock()
    Callback().snapshot_hard_limit_reached(event)
    NS.gluster.objects.NativeEvents.assert_called_with(
        'snapshot_hard_limit_reached|volume1',
        alert_notify=True,
        current_value='snapshot_hard_limit_reached',
        message='Snapshot hard limit reached for volume: '
                'volume1 in cluster '
                '77deef29-b8e5-4dc5-8247-21e2a409a66a',
        severity='warning'
    )


@patch.object(BaseObject, "load")
def test_compare_friend_volume_failed(load):
    init()
    load.return_value = maps.NamedDict(
        short_name="77deef29-b8e5-4dc5-8247-21e2a409a66a"
    )
    event = {"message": {"volume": "volume1"},
             "event": "COMPARE_FRIEND_VOLUME_FAILED",
             "ts": 1486634392,
             "nodeid": "0f5b4b99-fa4a-4b8f-be52-770b42879d67"
             }
    NS.gluster.objects.NativeEvents = MagicMock()
    Callback().compare_friend_volume_failed(event)
    NS.gluster.objects.NativeEvents.assert_called_with(
        'compare_friend_volume_failed|volume1',
        alert_notify=True,
        current_value='compare_friend_volume_failed',
        message='Compare friend volume failed for volume: '
                'volume1 in cluster '
                '77deef29-b8e5-4dc5-8247-21e2a409a66a',
        severity='warning'
    )


@patch.object(BaseObject, "load")
def test_posix_health_check_failed(load):
    init()
    load.return_value = maps.NamedDict(
        short_name="77deef29-b8e5-4dc5-8247-21e2a409a66a"
    )
    event = {"message": {"brick": "b1",
                         "path": "/gluster",
                         "error": "test_error",
                         "op": "testing"
                         },
             "event": "POSIX_HEALTH_CHECK_FAILED",
             "ts": 1486634392,
             "nodeid": "0f5b4b99-fa4a-4b8f-be52-770b42879d67"
             }
    NS.gluster.objects.NativeEvents = MagicMock()
    Callback().posix_health_check_failed(event)
    NS.gluster.objects.NativeEvents.assert_called_with(
        'posix_health_check_failed|b1/gluster',
        alert_notify=True,
        current_value='posix_health_check_failed',
        message='Posix health check failed for brick: b1. Path: '
                '/gluster in cluster '
                '77deef29-b8e5-4dc5-8247-21e2a409a66a. Error: '
                'test_error. op: testing',
        severity='warning'
    )


@patch.object(BaseObject, "load")
def test_peer_reject(load):
    init()
    load.return_value = maps.NamedDict(
        short_name="77deef29-b8e5-4dc5-8247-21e2a409a66a"
    )
    event = {"message": {"peer": "dhcp123-12.lab.abc.com"},
             "event": "PEER_REJECT",
             "ts": 1486634392,
             "nodeid": "0f5b4b99-fa4a-4b8f-be52-770b42879d67"
             }
    NS.gluster.objects.NativeEvents = MagicMock()
    Callback().peer_reject(event)
    NS.gluster.objects.NativeEvents.assert_called_with(
        'peer_reject|dhcp123-12.lab.abc.com',
        alert_notify=True,
        current_value='peer_reject',
        message='Peer: dhcp123-12.lab.abc.com is rejected in '
                'cluster 77deef29-b8e5-4dc5-8247-21e2a409a66a',
        severity='warning'
    )


@patch.object(BaseObject, "load")
def test_rebalance_status_update_failed(load):
    init()
    load.return_value = maps.NamedDict(
        short_name="77deef29-b8e5-4dc5-8247-21e2a409a66a"
    )
    event = {"message": {"volume": "v1"},
             "event": "REBALANCE_STATUS_UPDATE_FAILED",
             "ts": 1486634392,
             "nodeid": "0f5b4b99-fa4a-4b8f-be52-770b42879d67"
             }
    NS.gluster.objects.NativeEvents = MagicMock()
    Callback().rebalance_status_update_failed(event)
    NS.gluster.objects.NativeEvents.assert_called_with(
        'rebalance_status_update_failed|v1',
        alert_notify=True,
        current_value='rebalance_status_update_failed',
        message='Rebalance status update failed for volume: '
                'v1 in cluster '
                '77deef29-b8e5-4dc5-8247-21e2a409a66a',
        severity='warning'
    )


@patch.object(BaseObject, "load")
def test_svc_reconfigure_failed(load):
    init()
    load.return_value = maps.NamedDict(
        short_name="77deef29-b8e5-4dc5-8247-21e2a409a66a"
    )
    event = {"message": {"service": "testing",
                         "volume": "v1",
                         "svc_name": "test"},
             "event": "SVC_MANAGER_FAILED",
             "ts": 1486634392,
             "nodeid": "0f5b4b99-fa4a-4b8f-be52-770b42879d67"
             }
    NS.gluster.objects.NativeEvents = MagicMock()
    Callback().svc_reconfigure_failed(event)
    NS.gluster.objects.NativeEvents.assert_called_with(
        'svc_reconfigure_failed|testingv1',
        alert_notify=True,
        current_value='svc_reconfigure_failed',
        message='Service reconfigure failed for service: test',
        severity='warning'
    )


@patch.object(BaseObject, "load")
@patch.object(time, "strftime")
def test_georep_checkpoint_completed(strftime, load):
    init()
    load.return_value = maps.NamedDict(
        short_name="77deef29-b8e5-4dc5-8247-21e2a409a66a"
    )
    strftime.return_value = "01 Jan 1970 05:50:34"
    event = {"message": {"master_node": "node1",
                         "master_volume": "v1",
                         "brick_path": "/gluster/b1",
                         "slave_host": "node2",
                         "slave_volume": "volume1",
                         "checkpoint_time": "1234",
                         "checkpoint_completion_time": "2345"
                         },
             "event": "GEOREP",
             "ts": 1486634392,
             "nodeid": "0f5b4b99-fa4a-4b8f-be52-770b42879d67"
             }
    NS.gluster.objects.NativeEvents = MagicMock()
    Callback().georep_checkpoint_completed(event)
    NS.gluster.objects.NativeEvents.assert_called_with(
        'georep_checkpoint_completed|node1:v1:/gluster'
        '/b1--->node2:volume1',
        alert_notify=True,
        current_value='georep_checkpoint_completed',
        message='Georeplication checkpoint completed for pair '
                'node1:v1:/gluster/b1--->node2:volume1. '
                'Check point creation time 01 Jan 1970 05:50:34. '
                'Check point completion time 01 Jan 1970 05:50:34.'
                ' in cluster 77deef29-b8e5-4dc5-8247-21e2a409a66a',
        severity='info'
    )


@patch.object(BaseObject, "load")
@patch.object(time, "sleep")
def test_peer_detach(sleep, load):
    init()
    load.return_value = maps.NamedDict(
        short_name="77deef29-b8e5-4dc5-8247-21e2a409a66a"
    )
    sleep.return_value = True
    event = {"message": {"host": "dhcp123-12.lab.abc.com"},
             "event": "PEER_DISCONNECT",
             "ts": 1486634392,
             "nodeid": "0f5b4b99-fa4a-4b8f-be52-770b42879d67"
             }
    monitoring_utils.update_dashboard = MagicMock()
    Callback().peer_detach(event)
    monitoring_utils.update_dashboard.assert_called_with(
        'dhcp123-12.lab.abc.com',
        'host', '77deef29-b8e5-4dc5-8247-21e2a409a66a',
        'delete'
    )


@patch.object(time, "sleep")
@patch.object(BaseObject, "load_all")
@patch.object(BaseObject, "save")
@patch.object(BaseObject, "load")
def test_volume_delete(load, save, load_all, sleep):
    init()
    keys = maps.NamedDict(
        key="sub_vol1/dhcp123-12.lab.abc.com:|gluster|b1"
    )
    load.return_value = maps.NamedDict(
        brick_path="/gluster/b1",
        short_name="77deef29-b8e5-4dc5-8247-21e2a409a66a"
    )
    save.return_value = True
    obj = NS.gluster.objects.Volume()
    obj.name = "v1"
    obj.vol_id = "0f5b4b99-fa4a-4b8f-be52-770b42879d67"
    load_all.return_value = [obj]
    sleep.return_value = True
    monitoring_utils.update_dashboard = MagicMock()
    monitoring_utils.delete_resource_from_graphite = MagicMock()
    event = {"message": {"name": "v1"},
             "event": "VOLUME_DELETE",
             "ts": 1486634392,
             "nodeid": "0f5b4b99-fa4a-4b8f-be52-770b42879d67"
             }
    with patch.object(etcd_utils, "read") as read:
        read.return_value = maps.NamedDict(leaves=[keys], value="/gluster/b1")
        with patch.object(etcd_utils, "delete") as delete:
            delete.return_value = True
            Callback().volume_delete(event)
            monitoring_utils.delete_resource_from_graphite.assert_called_with(
                'v1',
                'volume',
                '77deef29-b8e5-4dc5-8247-21e2a409a66a',
                'delete'
            )
            monitoring_utils.update_dashboard.assert_called_with(
                'v1',
                'volume',
                '77deef29-b8e5-4dc5-8247-21e2a409a66a',
                'delete'
            )


def utils_read(param):
    if "indexes/ip" in param:
        return maps.NamedDict(
            value="0f5b4b99-fa4a-4b8f-be52-770b42879d67"
        )
    elif "NodeContext" in param:
        node_context = {"node_id": "0f5b4b99-fa4a-4b8f-be52-770b42879d67",
                        "fqdn": "127.0.0.1",
                        "tags": [],
                        "ipv4_addr": "127.0.0.1",
                        "status": "UP",
                        "sync_status": "processing",
                        "last_sync": "",
                        "pkey": ""}
        return maps.NamedDict(
            value=json.dumps(node_context)
        )


@patch.object(time, "sleep")
@patch.object(etcd_utils, "write")
@patch.object(etcd_utils, "refresh")
@patch.object(BaseObject, "load")
@patch.object(etcd_utils, "read", utils_read)
@patch.object(socket, "gethostbyname")
def test_volume_remove_brick_commit(
    hostbyname, load, refresh, write, sleep
):
    hostbyname.return_value = "127.0.0.1"
    sleep.return_value = True
    write.return_value = True
    refresh.return_value = True
    obj = NS.tendrl.objects.GlusterBrick(
        "77deef29-b8e5-4dc5-8247-21e2a409a66a",
        "dhcp123-12.lab.abc.com"
    )
    obj.vol_id = "0f5b4b99-fa4a-4b8f-be52-770b42879d67"
    obj.fqdn = "127.0.0.1"
    obj.ipv4_addr = "127.0.0.1"
    obj.tags = "127.0.0.1"
    obj.sync_status = ""
    obj.last_sync = ""
    obj.short_name = "77deef29-b8e5-4dc5-8247-21e2a409a66a"
    load.return_value = obj
    event = {"message": {"bricks": "dhcp123-12.lab.abc.com"
                         ":/gluster/b1",
                         "volume": "v1"},
             "event": "VOLUME_REMOVE_BRICK_FORCE",
             "ts": 1486634392,
             "nodeid": "0f5b4b99-fa4a-4b8f-be52-770b42879d67"
             }
    monitoring_utils.update_dashboard = MagicMock()
    monitoring_utils.delete_resource_from_graphite = MagicMock()
    with patch.object(etcd_utils, "delete") as delete:
        delete.return_value = True
        Callback().volume_remove_brick_commit(event)
    monitoring_utils.delete_resource_from_graphite.assert_called_with(
        'v1|127.0.0.1:/gluster/b1',
        'brick',
        '77deef29-b8e5-4dc5-8247-21e2a409a66a',
        'delete'
    )
    monitoring_utils.update_dashboard.assert_called_with(
        'v1|127.0.0.1:/gluster/b1',
        'brick',
        '77deef29-b8e5-4dc5-8247-21e2a409a66a',
        'delete'
    )


@patch.object(BaseObject, "load")
@patch.object(time, "sleep")
@patch.object(subprocess, "call")
@patch.object(etcd_utils, "read")
def test_snapshot_restored(read, call, sleep, load):
    load.return_value = maps.NamedDict(
        short_name="77deef29-b8e5-4dc5-8247-21e2a409a66a"
    )
    sleep.return_value = True
    call.return_value = True
    keys = maps.NamedDict(
        key="sub_vol1/dhcp123-12.lab.abc.com:|gluster|b1"
    )
    read.return_value = maps.NamedDict(leaves=[keys], value="/gluster/b1")
    event = {"message": {"volume_name": "GlusterVolume"},
             "event": "SNAPSHOT_RESTORED",
             "ts": 1486634392,
             "nodeid": "0f5b4b99-fa4a-4b8f-be52-770b42879d67"
             }
    raw_data = ini2json.ini_to_dict(
        os.path.join(os.path.dirname(__file__),
                     "gluster_state.yaml")
    )
    with patch.object(ini2json, "ini_to_dict") as ini_to_dict:
        ini_to_dict.return_value = raw_data
        obj = Callback()
        with patch.object(obj, "volume_remove_brick_force") as rm_brick:
            obj.snapshot_restored(event)
            rm_brick.assert_called_with(
                {'event': 'SNAPSHOT_RESTORED',
                 'ts': 1486634392,
                 'message': {'volume': 'GlusterVolume',
                             'bricks': '/gluster/b1'
                             },
                 'nodeid': '0f5b4b99-fa4a-4b8f-be52-770b42879d67'
                 }
            )
