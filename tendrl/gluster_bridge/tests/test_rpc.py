import json
from mock import MagicMock
import sys
sys.modules['tendrl.bridge_common.JobValidator'] = MagicMock()
sys.modules['tendrl.gluster_bridge.persistence.sds_operation'] = MagicMock()
sys.modules['tendrl.gluster_bridge.config'] = MagicMock()
sys.modules['etcd'] = MagicMock()
from tendrl.gluster_bridge.manager import rpc
del sys.modules['tendrl.gluster_bridge.config']
del sys.modules['etcd']
del sys.modules['tendrl.gluster_bridge.persistence.sds_operation']
del sys.modules['tendrl.bridge_common.JobValidator']
import uuid


class Test_EtcdThread(object):
    def setup_method(self, method):
        self.cluster_id = 'aa22a6fe-87f0-45cf-8b70-2d0ff4c02af6'
        mock_job = Mock_Job(self.cluster_id)
        self.children = [mock_job]
        self.manager = MagicMock()
        self.count = 0
        self.etcdthread = rpc.EtcdThread(self.manager)
        self.etcdthread._complete = self
        self.etcdthread._server.client.read = MagicMock(return_value=self)
        self.etcdthread._server.client.write = MagicMock()

    def test_etcdthread(self, monkeypatch):
        def mock_uuid4():
            return self.cluster_id
        monkeypatch.setattr(uuid, 'uuid4', mock_uuid4)
        self.etcdthread._server.bridge_id = self.cluster_id
        self.etcdthread._complete = self
        rpc.api.JobValidator = MagicMock(return_value=self)
        self.etcdthread._run()
        self.etcdthread._server.client.write.assert_called()

    def test_stop(self):
        self.etcdthread._complete.set = MagicMock()
        self.etcdthread.stop()
        self.etcdthread._complete.set.assert_called()

    def is_set(self):
        if self.count < 2:
            self.count = self.count + 1
            return False
        else:
            return True

    def wait(self, *args):
        return

    def validateApi(*args):
        return 0, "mock"


class Mock_Job(object):
    def __init__(self, cluster_id):
        self.key = "pytest"
        self.value = json.dumps(
            {
                "status": "new",
                "request_id": 12345,
                "sds_name": "gluster",
                "flow": "createVolume",
                "cluster_id": cluster_id
            })
