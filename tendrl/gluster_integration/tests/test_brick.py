import etcd
import json

from mock import MagicMock

from tendrl.gluster_integration.objects.volume import brick


class TestBrick(object):
    def test_refresh(self):
        brick.etcd = MagicMock()
        brick.TendrlConfig = MagicMock()
        self.obj = brick.Brick("clusterid1", "volumeid1", "brick1")
        str1 = """{"action": "get",
        "node": {"key":
        "/clusters/clusterid1/Volumes/volumeid1/Bricks/host1:brick1",
        "dir": true,
        "nodes":[
        {"key":
        "/clusters/clusterid1/Volumes/volumeid1/Bricks/host1:brick1/port",
        "value": "None",
        "modifiedIndex": 1,
        "createdIndex": 324602
        },
        {
        "key":
        "/clusters/clusterid1/Volumes/volumeid1/Bricks/host1:brick1/status",
        "value": "",
        "modifiedIndex": 1,
        "createdIndex": 1
        },
        {
        "key":
        "/clusters/clusterid1/Volumes/volumeid1/Bricks/host1:brick1/vol_id",
        "value": "volumeid1",
        "modifiedIndex": 1,
        "createdIndex": 1
        },
        {
        "key":
        "/clusters/clusterid1/Volumes/volumeid1/Bricks/host1:brick1/cluster_id",
        "value": "clusterid1",
        "modifiedIndex": 1,
        "createdIndex": 1
        },
        {
        "key":
        "/clusters/clusterid1/Volumes/volumeid1/Bricks/host1:brick1/fs_type",
        "value": "",
        "modifiedIndex": 1,
        "createdIndex": 1
        },
        {
        "key":
        "/clusters/clusterid1/Volumes/volumeid1/Bricks/host1:brick1/hostname",
        "value": "host1",
        "modifiedIndex": 1,
        "createdIndex": 1
        },
        {
        "key":
        "/clusters/clusterid1/Volumes/volumeid1/Bricks/host1:brick1/mount_opts",
        "value": "",
        "modifiedIndex": 1,
        "createdIndex": 1
        },
        {
        "key":
        "/clusters/clusterid1/Volumes/volumeid1/Bricks/host1:brick1/path",
        "value": "host1:brick1",
        "modifiedIndex": 1,
        "createdIndex": 1
        }
        ],
        "modifiedIndex": 1,
        "createdIndex": 1}}"""
        result = json.loads(str1.decode('utf-8'))
        self.obj.client.read = MagicMock(
            return_value=etcd.EtcdResult(**result)
        )
        self.obj.refresh()

        self.obj.client.read.assert_called_with(
            "/clusters/clusterid1/Volumes/volumeid1/Bricks/brick1"
        )
        assert self.obj.cluster_id == "clusterid1"
        assert self.obj.vol_id == "volumeid1"
        assert self.obj.path == "host1:brick1"
        assert self.obj.hostname == "host1"
