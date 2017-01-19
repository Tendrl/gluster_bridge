from mock import MagicMock
import sys
sys.modules[
    'tendrl.gluster_integration.persistence.sync_objects'] = MagicMock()
from tendrl.gluster_integration.persistence import persister
del sys.modules[
    'tendrl.gluster_integration.persistence.sync_objects']
from tendrl.gluster_integration.persistence import servers


class Test_Persister(object):
    def setup_method(self, method):
        self.Persister = persister.GlusterIntegrationEtcdPersister(MagicMock())

    def test_Persister_Creation(self):
        assert self.Persister is not None

    def test_update_sync_object(self):
        """Sending dummy parameters"""
        data = "data"
        updated = "updated"
        self.Persister.update_sync_object(
            updated,
            '145b9021-d47c-4094-957b-7545e8232ab7',
            data)
        self.Persister.etcd_orm.save.assert_called()

    def test_update_peer(self):
        self.peer = servers.Peer
        self.Persister.update_peer(self.peer)
        self.Persister.etcd_orm.save.assert_called_with(
            self.peer
            )

    def test_update_volume(self):
        self.volume = servers.Volume
        self.Persister.update_volume(self.volume)
        self.Persister.etcd_orm.save.assert_called_with(
            self.volume
            )

    def test_update_brick(self):
        self.brick = servers.Brick
        self.Persister.update_brick(self.brick)
        self.Persister.etcd_orm.save.assert_called_with(
            self.brick
            )

    def test_save_events(self):
        self.Persister.save_events(["event"])
        self.Persister.etcd_orm.save.assert_called_with(
            "event"
            )
