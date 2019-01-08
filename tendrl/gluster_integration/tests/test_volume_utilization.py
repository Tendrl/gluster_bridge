import importlib
import inspect
import maps
import mock

from tendrl.gluster_integration.objects.utilization import Utilization
from tendrl.gluster_integration.objects.volume import Volume
from tendrl.gluster_integration.sds_sync import utilization


@mock.patch(
    'subprocess.Popen.communicate',
    mock.Mock(return_value=('{"success":{"vol1": {"total": "20000", "used": "5000",\
        "pcnt_used": "25", "total_inode": "100", "used_inode": "20",\
        "pcnt_inode_used": "20"}, "vol2": {"total": "30000", "used": "6000",\
        "pcnt_used": "50", "total_inode": "100", "used_inode": "20",\
        "pcnt_inode_used": "20"}}}', ""))
)
@mock.patch(
    'tendrl.gluster_integration.objects.volume.Volume.save',
    mock.Mock(return_value=None)
)
@mock.patch(
    'tendrl.gluster_integration.objects.utilization.Utilization.save',
    mock.Mock(return_value=None)
)
@mock.patch(
    'tendrl.commons.utils.log_utils.log',
    mock.Mock(return_value=None)
)
@mock.patch(
    'tendrl.commons.objects.BaseObject.__init__',
    mock.Mock(return_value=None)
)
def test_sync_volume_utilization_details_with_started_volume():
    setattr(NS, "publisher_id", "gluster-integration")
    setattr(NS, "gluster", maps.NamedDict())
    NS.gluster["objects"] = maps.NamedDict()
    obj = importlib.import_module(
        'tendrl.gluster_integration.objects.utilization'
    )
    for obj_cls in inspect.getmembers(obj, inspect.isclass):
        NS.gluster.objects["Utilization"] = obj_cls[1]
    volumes = []
    volumes.append(Volume(
        vol_id='vol-id',
        vol_type='Replicate',
        name='vol1',
        status='Started',
        state='up',
        brick_count=3,
        replica_count=3,
        subvol_count=1,
    ))
    volumes.append(Volume(
        vol_id='vol-id',
        vol_type='Replicate',
        name='vol2',
        status='Started',
        state='up',
        brick_count=3,
        replica_count=3,
        subvol_count=1,
    ))
    utilization.sync_utilization_details(volumes)
    with mock.patch.object(Volume, 'save') as vol_save_mock:
        vol_save_mock.assert_called
        for volume in volumes:
            if volume.name == "vol1":
                assert volume.usable_capacity == 20000
                assert volume.used_capacity == 5000
                assert volume.pcnt_used == '25'
                assert volume.total_inode_capacity == 100
                assert volume.used_inode_capacity == 20
                assert volume.pcnt_inode_used == '20'
            elif volume.name == "vol2":
                assert volume.usable_capacity == 30000
                assert volume.used_capacity == 6000
                assert volume.pcnt_used == '50'
                assert volume.total_inode_capacity == 100
                assert volume.used_inode_capacity == 20
                assert volume.pcnt_inode_used == '20'

    with mock.patch.object(Utilization, 'save') as util_save_mock:
        util_save_mock.assert_called


@mock.patch(
    'subprocess.Popen.communicate',
    mock.Mock(return_value=('{"success":{"vol1": {"total": "20000", "used": "5000",\
        "pcnt_used": "25", "total_inode": "100", "used_inode": "20",\
        "pcnt_inode_used": "20"}, "vol2": {"total": "20000", "used": "5000",\
        "pcnt_used": "25", "total_inode": "100", "used_inode": "20",\
        "pcnt_inode_used": "20"}}}', ""))
)
@mock.patch(
    'tendrl.gluster_integration.objects.volume.Volume.save',
    mock.Mock(return_value=None)
)
@mock.patch(
    'tendrl.gluster_integration.objects.utilization.Utilization.save',
    mock.Mock(return_value=None)
)
@mock.patch(
    'tendrl.commons.utils.log_utils.log',
    mock.Mock(return_value=None)
)
@mock.patch(
    'tendrl.commons.objects.BaseObject.__init__',
    mock.Mock(return_value=None)
)
def test_sync_volume_utilization_details_with_started_volume1():
    setattr(NS, "publisher_id", "gluster-integration")
    setattr(NS, "gluster", maps.NamedDict())
    NS.gluster["objects"] = maps.NamedDict()
    obj = importlib.import_module(
        'tendrl.gluster_integration.objects.utilization'
    )
    for obj_cls in inspect.getmembers(obj, inspect.isclass):
        NS.gluster.objects["Utilization"] = obj_cls[1]
    volume1 = Volume(
        vol_id='vol-id1',
        vol_type='Replicate',
        name='vol1',
        status='Started',
        state='up',
        brick_count=3,
        replica_count=3,
        subvol_count=1,
    )
    volume2 = Volume(
        vol_id='vol-id2',
        vol_type='Replicate',
        name='vol2',
        status='Started',
        state='up',
        brick_count=3,
        replica_count=3,
        subvol_count=1,
    )
    utilization.sync_utilization_details([volume1, volume2])
    with mock.patch.object(Volume, 'save') as vol_save_mock:
        vol_save_mock.assert_called
        assert volume1.usable_capacity == 20000
        assert volume1.used_capacity == 5000
        assert volume1.pcnt_used == '25'
        assert volume1.total_inode_capacity == 100
        assert volume1.used_inode_capacity == 20
        assert volume1.pcnt_inode_used == '20'
        assert volume2.usable_capacity == 20000
        assert volume2.used_capacity == 5000
        assert volume2.pcnt_used == '25'
        assert volume2.total_inode_capacity == 100
        assert volume2.used_inode_capacity == 20
        assert volume2.pcnt_inode_used == '20'

    with mock.patch.object(Utilization, 'save') as util_save_mock:
        util_save_mock.assert_called


@mock.patch(
    'tendrl.gluster_integration.objects.volume.Volume.save',
    mock.Mock(return_value=None)
)
@mock.patch(
    'tendrl.gluster_integration.objects.utilization.Utilization.save',
    mock.Mock(return_value=None)
)
@mock.patch(
    'tendrl.commons.utils.log_utils.log',
    mock.Mock(return_value=None)
)
@mock.patch(
    'tendrl.commons.objects.BaseObject.__init__',
    mock.Mock(return_value=None)
)
def test_sync_volume_utilization_details_with_stopped_volume():
    setattr(NS, "publisher_id", "gluster-integration")
    setattr(NS, "gluster", maps.NamedDict())
    NS.gluster["objects"] = maps.NamedDict()
    obj = importlib.import_module(
        'tendrl.gluster_integration.objects.utilization'
    )
    for obj_cls in inspect.getmembers(obj, inspect.isclass):
        NS.gluster.objects["Utilization"] = obj_cls[1]
    volume = Volume(
        vol_id='vol-id',
        vol_type='Replicate',
        name='vol1',
        status='Stopped',
        state='up',
        brick_count=3,
        replica_count=3,
        subvol_count=1,
    )
    utilization.sync_utilization_details([volume])
    with mock.patch.object(Volume, 'save') as vol_save_mock:
        assert not vol_save_mock.called

    with mock.patch.object(Utilization, 'save') as util_save_mock:
        assert not util_save_mock.called
