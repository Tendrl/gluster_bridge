"""
test_gluster_bridge
----------------------------------

Tests for `gluster_bridge` module.
"""
from mock import MagicMock
import os
import shutil
import sys
import tempfile
sys.modules['tendrl.bridge_common.JobValidator'] = MagicMock()
sys.modules['tendrl.bridge_common'] = MagicMock()
sys.modules['tendrl.gluster_bridge.persistence.servers'] = MagicMock()
sys.modules['tendrl.gluster_bridge.persistence.sds_operation'] = MagicMock()
sys.modules['tendrl.gluster_bridge.config'] = MagicMock()
sys.modules['tendrl.gluster_bridge.manager.rpc'] = MagicMock()
sys.modules['tendrl.gluster_bridge.persistence.persister'] = MagicMock()
sys.modules['logging'] = MagicMock()
from tendrl.gluster_bridge.manager import manager
del sys.modules['tendrl.gluster_bridge.manager.rpc']
del sys.modules['tendrl.gluster_bridge.persistence.persister']
del sys.modules['logging']
del sys.modules['tendrl.gluster_bridge.config']
del sys.modules['tendrl.bridge_common']
del sys.modules['tendrl.gluster_bridge.persistence.servers']
del sys.modules['tendrl.gluster_bridge.persistence.sds_operation']
del sys.modules['tendrl.bridge_common.JobValidator']


class TestGluster_bridge(object):
    def setup_method(self, method):
        manager.gevent.sleep = MagicMock()
        manager.Peer = MagicMock()
        manager.Brick = MagicMock()
        manager.Volume = MagicMock()
        manager.log = MagicMock()
        self.managerobj = manager.Manager()
        self.Manager = manager
        self.tempdir = tempfile.mkdtemp()

    def teardown_method(self, method):
        shutil.rmtree(self.tempdir)

    def _makeFile(self, filename, text):
        filename = os.path.join(self.tempdir, filename)
        f = open(filename, 'w')
        f.write(text)
        return filename
