from mock import MagicMock
import pytest
import sys
sys.modules['tendrl.gluster_bridge.config'] = MagicMock()
sys.modules['logging'] = MagicMock()
from tendrl.gluster_bridge.persistence import persister
del sys.modules['tendrl.gluster_bridge.config']
del sys.modules['logging']
from tendrl.gluster_bridge.persistence import servers


class Test_deferred_call(object):
    def setup_method(self, method):
        self.fn = MagicMock()
        self.deferred_call = persister.deferred_call(
            self.fn, ["pytest"], {"test": "pytest"}
            )

    def test_deferred_call(self):
        self.deferred_call.call_it()
        self.deferred_call.fn.assert_called_with(
            "pytest", test="pytest"
            )


class Test_Persister(object):
    def setup_method(self, method):
        persister.etcd_server = MagicMock()
        self.Persister = persister.Persister()

    def test_Persister_Creation(self):
        assert self.Persister is not None

    def test_getattribute_(self):
        """Check __getattribute__ with dummy function"""
        with pytest.raises(AttributeError):
            self.Persister.test_func()
        """Check __getattribute__ raise excption for variable"""
        self.Persister._testing = None
        with pytest.raises(AttributeError):
            self.Persister.testing()
        persister.deferred_call = MagicMock(return_value=None)
        with pytest.raises(Exception):
            self.Persister.update_peer()

    def test_update_sync_object(self):
        """Sending dummy parameters"""
        data = "data"
        updated = "updated"
        self.Persister._update_sync_object(updated, data)
        self.Persister._store.save.assert_called()

    def test_update_peer(self):
        self.peer = servers.Peer
        self.Persister._update_peer(self.peer)
        self.Persister._store.save.assert_called_with(
            self.peer
            )

    def test_update_volume(self):
        self.volume = servers.Volume
        self.Persister._update_volume(self.volume)
        self.Persister._store.save.assert_called_with(
            self.volume
            )

    def test_update_brick(self):
        self.brick = servers.Brick
        self.Persister._update_brick(self.brick)
        self.Persister._store.save.assert_called_with(
            self.brick
            )

    def test_save_events(self):
        self.Persister._save_events(["event"])
        self.Persister._store.save.assert_called_with(
            "event"
            )

    def test_run(self, monkeypatch):
        def stop_while_loop(temp):
            self.Persister.stop()
        assert self.Persister._complete.is_set() is False
        monkeypatch.setattr(persister.gevent, 'sleep', stop_while_loop)
        self.Persister._run()
        assert self.Persister._complete.is_set() is True
