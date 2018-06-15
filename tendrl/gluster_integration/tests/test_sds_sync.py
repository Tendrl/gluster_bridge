import etcd
import importlib

from mock import MagicMock
from mock import mock
from mock import patch

from tendrl.commons.objects import BaseObject
from tendrl.commons.utils import etcd_utils
from tendrl.commons.utils import event_utils

from tendrl.gluster_integration.sds_sync import check_peers
from tendrl.gluster_integration.sds_sync import check_volumes
from tendrl.gluster_integration.sds_sync import cluster_not_ready
from tendrl.gluster_integration.sds_sync import get_volume_alert_counts
from tendrl.gluster_integration.sds_sync import sync_by_provisioner


@patch.object(BaseObject, "save")
@patch.object(BaseObject, "load_all")
@patch.object(event_utils, "emit_event")
@patch.object(BaseObject, "hash_compare_with_central_store")
@patch.object(etcd_utils, "refresh")
def test_brick_status_alert(
    compare, refresh, emit_event, load_all, save
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


@patch.object(BaseObject, "load_all")
def test_get_volume_alert_counts(load_all):

    # help objects
    gluster_volume_a = NS.tendrl.objects.GlusterVolume(
        integration_id="77deef29-b8e5-4dc5-8247-21e2a409a66a",
        vol_id=1,
        name='a'
    )
    gluster_volume_b = NS.tendrl.objects.GlusterVolume(
        integration_id="77deef29-b8e5-4dc5-8247-21e2a409a66a",
        vol_id=2,
        name='b'
    )

    # List returned with the information of all volumes
    load_all.return_value = [gluster_volume_a, gluster_volume_b]

    # Check all alarms in all volumes are set to 0
    expected_result = {'a': {'vol_id': 1, 'alert_count': 0},
                       'b': {'vol_id': 2, 'alert_count': 0}}

    result = get_volume_alert_counts()
    assert result == expected_result


def test_cluster_not_ready():

    mock_cluster = MagicMock()
    mock_cluster.current_job = {}

    # Scene 1: Cluster not ready - condition 1
    mock_cluster.status = "importing"
    mock_cluster.current_job["status"] = "failed"
    assert cluster_not_ready(mock_cluster)

    # Scene 2: Cluster not ready - condition 2
    mock_cluster.status = "unmanaging"
    mock_cluster.current_job["status"] = "it does not matter"
    assert cluster_not_ready(mock_cluster)

    # Scene 3: Cluster not ready - condition 3
    mock_cluster.status = "set_volume_profiling"
    mock_cluster.current_job["status"] = "it does not matter"
    assert cluster_not_ready(mock_cluster)

    # Scene 4: Cluster ready
    mock_cluster.status = "other thing"
    mock_cluster.current_job["status"] = "it does not matter"
    assert not cluster_not_ready(mock_cluster)


@patch.object(BaseObject, "load")
@patch.object(BaseObject, "save")
@patch.object(event_utils, "emit_event")
def test_check_peers(mock_emit_event,
                     mock_baseobject_save,
                     mock_baseobject_load):

    state_raw_data = {"Peers": {"peer1.uuid": 1,
                                "peer1.primary_hostname": "p1",
                                "peer1.state": "the_state",
                                "peer1.connected": "Connected",
                                "peer2.uuid": 2,
                                "peer2.primary_hostname": "p2",
                                "peer2.state": "the_state",
                                "peer2.connected": "Connected"}}
    sync_time = 1

    mock_baseobject_save.return_value = True

    peer_mock = MagicMock()
    mock_baseobject_load.return_value = peer_mock

    # Scene 1: No discordance between peer data retrieved from gluster and peer
    # data retrieved from etcd

    peer_mock.connected = "Connected"
    synctime = check_peers("test_cluster", state_raw_data, sync_time)

    # two peers checked
    assert synctime == 11

    # no problems detected
    mock_emit_event.assert_not_called()

    # Scene 2. We have discordance between the different sources
    # etcd values differs from gluster retrieved data
    # But gluster info points that the peer is connected so INFO message should
    # be delivered
    peer_mock.connected = "Disconnected"  # <--- read from etcd
    mock_baseobject_load.return_value = peer_mock
    mock_emit_event.reset_mock()

    synctime = check_peers("test_cluster", state_raw_data, sync_time)

    # two peers checked
    assert synctime == 11

    # Right kind of events emited
    mock_emit_event.assert_called_with(
        "peer_status",
        "Connected",
        "Peer p2 in cluster test_cluster is Connected",
        "peer_p2",
        "INFO")

    # Scene 3: We have discordance between the different sources
    # gluster retrieved data shows a peer Disconnected.
    # Warning message should be delivered

    # this is read from etcd db
    peer_mock.connected = "Connected"
    # this is obtained from gluster cmd
    state_raw_data["Peers"]["peer2.connected"] = "Disconnected"

    mock_baseobject_load.return_value = peer_mock
    mock_emit_event.reset_mock()

    synctime = check_peers("test_cluster", state_raw_data, sync_time)

    # two peers checked
    assert synctime == 11

    # Right kind of events emited
    mock_emit_event.assert_called_with(
        "peer_status",
        "Disconnected",
        "Peer p2 in cluster test_cluster is Disconnected",
        "peer_p2",
        "WARNING")


@mock.patch('tendrl.gluster_integration.sds_sync.sync_volumes')
@patch.object(BaseObject, "load")
def test_check_volumes(mock_glusterVolume_load, mock_sync_volumes):

    # Scene 1: One volume with options
    sync_ttl = 1
    cluster_short_name = "test_cluster"
    raw_data = {"Volumes": {"volume1.id": 1,
                            "volume1.status": "Ok",
                            "volume1.name": "vol1",
                            "volume1.options.count": 2,
                            "volume1.options.value2": "trusted.glusterfs.dht",
                            "volume1.options.key2": "cluster.dht-xattr-name",
                            "volume1.options.value1": "(null)",
                            "volume1.options.key1": "cluster.extra-hash-regex"}
                }
    raw_data_options = {"Volume Options": "volume options"}

    mock_sync_volumes.side_effect = [True, KeyError]

    the_volume = MagicMock()
    the_volume.options = {"cluster.min-free-inodes": "5%"}

    mock_glusterVolume_load.return_value = the_volume

    sync_ttl = check_volumes(cluster_short_name,
                             raw_data, raw_data_options, sync_ttl)

    # Only 1 Volume
    assert sync_ttl == 2

    # The Volume is loaded, it will be needed to add options coming from
    # gluster cmd details
    mock_glusterVolume_load.assert_called()

    # 5 options for the volume must be saved (probably this can be improved
    # saving only one time all the options)
    assert len(the_volume.mock_calls) == 5

    # Scene 2: One volume without options (is right not to update options
    # when there is no options in the loaded volume?)
    sync_ttl = 1
    mock_sync_volumes.side_effect = [True, KeyError]

    the_volume.reset_mock()
    the_volume.options = None
    mock_glusterVolume_load.reset_mock()
    mock_glusterVolume_load.return_value = the_volume

    sync_ttl = check_volumes(cluster_short_name, raw_data, raw_data_options,
                             sync_ttl)

    # Only 1 Volume
    assert sync_ttl == 2

    # The Volume is loaded, it will be needed to add options coming from
    # gluster cmd details
    mock_glusterVolume_load.assert_called()

    # No options for the volume will be saved
    assert len(the_volume.mock_calls) == 0


@mock.patch(
    "tendrl.gluster_integration.sds_sync.cluster_status.sync_cluster_status")
@mock.patch(
    "tendrl.gluster_integration.sds_sync.utilization.sync_utilization_details")
@mock.patch(
    "tendrl.gluster_integration.sds_sync.client_connections.sync_volume_connections")
@mock.patch(
    "tendrl.gluster_integration.sds_sync.georep_details.aggregate_session_status")
@mock.patch(
    "tendrl.gluster_integration.sds_sync.rebalance_status.sync_volume_rebalance_status")
@mock.patch(
    "tendrl.gluster_integration.sds_sync.rebalance_status.sync_volume_rebalance_estimated_time")
@mock.patch(
    "tendrl.gluster_integration.sds_sync.snapshots.sync_volume_snapshots")
@mock.patch(
    "tendrl.gluster_integration.message.process_events.process_events")
@patch.object(BaseObject, "load_all")
def test_sync_by_provisioner(mock_gluster_volume_load_all,
                             mock_evt,
                             mock_sync_volume_snapshots,
                             mock_sync_volume_rebalance_estimated_time,
                             mock_sync_volume_rebalance_status,
                             mock_aggregate_session_status,
                             mock_sync_volume_connections,
                             mock_sync_utilization_details,
                             mock_sync_cluster_status):

    raw_data = {"Volumes": {"volume1.id": 1,
                            "volume1.status": "Ok",
                            "volume1.name": "vol1",
                            "volume1.options.count": 2,
                            "volume1.options.value2": "trusted.glusterfs.dht",
                            "volume1.options.key2": "cluster.dht-xattr-name",
                            "volume1.options.value1": "(null)",
                            "volume1.options.key1": "cluster.extra-hash-regex"}
                }

    # Sync a couple of volumes
    integration_id = "77deef29-b8e5-4dc5-8247-21e2a409a66a"
    node_context = MagicMock()
    node_context.tags = "provisioner/%s" % integration_id
    sync_ttl = 1

    volume_1 = MagicMock()
    volume_2 = MagicMock()
    volumes_returned = [volume_1, volume_2]

    i = 1
    for volume in volumes_returned:
        volume.id = i
        volume.deleted = False
        volume.current_job = {"status": ""}
        i += 1

    mock_gluster_volume_load_all.return_value = volumes_returned

    sync_by_provisioner(integration_id, node_context, raw_data, sync_ttl)

    # Check That diferent sync method are called with the right parameters
    # cluster_status.sync_cluster_status
    assert len(mock_sync_cluster_status.call_args[0][0]) == 2
    assert [v.id for v in mock_sync_cluster_status.call_args[0][0]] == [1, 2]
    assert mock_sync_cluster_status.call_args[0][1] == 351

    for mock_fn in [mock_sync_utilization_details,
                    mock_sync_volume_connections,
                    mock_sync_volume_rebalance_estimated_time,
                    mock_sync_volume_rebalance_status,
                    ]:
        assert len(mock_fn.call_args[0][0]) == 2
        assert [v.id for v in mock_fn.call_args[0][0]] == [1, 2]

    # georep_details.aggregate_session_status
    assert mock_aggregate_session_status.called

    # process_events.process_events
    assert mock_evt.called

    # snapshots.sync_volume_snapshots
    # 18 = 10 (default sync interval) + (2 volumes * 4)
    mock_sync_volume_snapshots.assert_called_with(raw_data["Volumes"], 18)
