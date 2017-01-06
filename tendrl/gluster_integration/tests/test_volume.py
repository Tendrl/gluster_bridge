import etcd
import json

from mock import MagicMock

from tendrl.gluster_integration.objects.volume import brick
from tendrl.gluster_integration.objects.volume import volume


class TestVolume(object):
    def test_refresh_with_volume_id(self, monkeypatch):
        volume.etcd = MagicMock()
        volume.TendrlConfig = MagicMock()
        volume.brick.etcd = MagicMock()
        volume.brick.TendrlConfig = MagicMock()
        self.obj = volume.Volume("", "volumeid1", "clusterid1")
        self.obj.client.read = MagicMock()
        brick_ins = brick.Brick("clusterid1", "volumeid1", "host1:brick1")

        def mock_etcd_read(param):
            if param == "/clusters/clusterid1/Volumes/volumeid1":
                str = """{"action": "get",
                "node": {"key":
                "/clusters/clusterid1/Volumes/volumeid1",
                "dir": true,
                "nodes": [
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1/status",
                "value": "Started",
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1/vol_id",
                "value": "volumeid1",
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1/vol_type",
                "value": "Distribute",
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1/Bricks",
                "dir": true,
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1/brick_count",
                "value": "2",
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1/cluster_id",
                "value": "clusterid1",
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1/deleted",
                "value": "",
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1/name",
                "value": "vol1",
                "modifiedIndex": 1,
                "createdIndex": 1
                }
                ],
                "modifiedIndex": 1,
                "createdIndex": 1}}"""
                result = json.loads(str.decode('utf-8'))
                return etcd.EtcdResult(**result)
            elif param == "/clusters/clusterid1/Volumes/volumeid1/Bricks":
                str = """{"action": "get",
                "node": {"key":
                "/clusters/clusterid1/Volumes/volumeid1/Bricks",
                "dir": true,
                "nodes": [
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1/Bricks/host1:brick1",
                "dir": true,
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1/Bricks/host1:brick2",
                "dir": true,
                "modifiedIndex": 1,
                "createdIndex": 1
                }
                ],
                "modifiedIndex": 1,
                "createdIndex": 1}}"""
                result = json.loads(str.encode('utf-8'))
                return etcd.EtcdResult(**result)
            elif param == \
                "/clusters/clusterid1/Volumes/volumeid1/Bricks/host1:brick1":
                str = """{"action": "get",
                "node": {"key":
                "/clusters/clusterid1/Volumes/volumeid1/Bricks/host1:brick1",
                "dir": true,
                "nodes": [
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1/Bricks/host1:brick1/path",
                "value": "host1:brick1",
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1/Bricks/host1:brick1/port",
                "value": "0",
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1/Bricks/host1:brick1/status",
                "value": "Started",
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1/Bricks/host1:brick1/vol_id",
                "value": "volumeid1",
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1/Bricks/host1:brick1/cluster_id",
                "value": "clusterid1",
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1/Bricks/host1:brick1/fs_type",
                "value": "",
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1/Bricks/host1:brick1/hostname",
                "value": "host1",
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1/Bricks/host1:brick1/mount_opts",
                "value": "",
                "modifiedIndex": 1,
                "createdIndex": 1
                }
                ],
                "modifiedIndex": 1,
                "createdIndex": 1}}"""
                result = json.loads(str.encode('utf-8'))
                return etcd.EtcdResult(**result)
            elif param == \
                "/clusters/clusterid1/Volumes/volumeid1/Bricks/host1:brick2":
                str = """{"action": "get",
                "node": {"key":
                "/clusters/clusterid1/Volumes/volumeid1/Bricks/host1:brick2",
                "dir": true,
                "nodes": [
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1/Bricks/host1:brick2/path",
                "value": "host1:brick2",
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1/Bricks/host1:brick2/port",
                "value": "0",
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1/Bricks/host1:brick2/status",
                "value": "Started",
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1/Bricks/host1:brick2/vol_id",
                "value": "volumeid1",
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1/Bricks/host1:brick2/cluster_id",
                "value": "clusterid1",
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1/Bricks/host1:brick2/fs_type",
                "value": "",
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1/Bricks/host1:brick2/hostname",
                "value": "host1",
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1/Bricks/host1:brick2/mount_opts",
                "value": "",
                "modifiedIndex": 1,
                "createdIndex": 1
                }
                ],
                "modifiedIndex": 1,
                "createdIndex": 1}}"""
                result = json.loads(str.encode('utf-8'))
                return etcd.EtcdResult(**result)

        monkeypatch.setattr(
            self.obj.client,
            'read',
            mock_etcd_read
        )
        monkeypatch.setattr(
            brick_ins.client,
            'read',
            mock_etcd_read
        )

        self.obj.refresh()

        assert self.obj.name == "vol1"
        assert self.obj.vol_type == "Distribute"
        assert self.obj.brick_count == "2"
        assert self.obj.deleted == ""
        assert self.obj.status == "Started"
        assert len(self.obj.bricks) == 2
        assert self.obj.bricks[0].path == "host1:brick1"
        assert self.obj.bricks[0].vol_id == "volumeid1"
        assert self.obj.bricks[0].cluster_id == "clusterid1"
        assert self.obj.bricks[0].hostname == "host1"
        assert self.obj.bricks[0].status == "Started"
        assert self.obj.bricks[1].path == "host1:brick2"
        assert self.obj.bricks[1].vol_id == "volumeid1"
        assert self.obj.bricks[1].cluster_id == "clusterid1"
        assert self.obj.bricks[1].hostname == "host1"
        assert self.obj.bricks[1].status == "Started"

    def test_refresh_with_volume_name(self, monkeypatch):
        volume.etcd = MagicMock()
        volume.TendrlConfig = MagicMock()
        self.obj = volume.Volume("vol1", "", "clusterid1")
        self.obj.client.read = MagicMock()
        self.obj.__populate_brick_details__ = MagicMock()

        def mock_etcd_read(param):
            if param == "/clusters/clusterid1/Volumes":
                str = """{"action": "get",
                "node": {"key":
                "/clusters/clusterid1/Volumes",
                "dir": true,
                "nodes": [
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1",
                "dir": true,
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid2",
                "dir": true,
                "modifiedIndex": 1,
                "createdIndex": 1
                }
                ],
                "modifiedIndex": 1,
                "createdIndex": 1}}"""
                result = json.loads(str.decode('utf-8'))
                return etcd.EtcdResult(**result)
            elif param == "/clusters/clusterid1/Volumes/volumeid1":
                str = """{"action": "get",
                "node": {"key":
                "/clusters/clusterid1/Volumes/volumeid1",
                "dir": true,
                "nodes": [
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1/status",
                "value": "Started",
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1/vol_id",
                "value": "volumeid1",
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1/vol_type",
                "value": "Distribute",
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1/Bricks",
                "dir": true,
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1/brick_count",
                "value": "3",
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1/cluster_id",
                "value": "clusterid1",
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1/deleted",
                "value": "",
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid1/name",
                "value": "vol1",
                "modifiedIndex": 1,
                "createdIndex": 1
                }
                ],
                "modifiedIndex": 1,
                "createdIndex": 1}}"""
                result = json.loads(str.decode('utf-8'))
                return etcd.EtcdResult(**result)
            elif param == "/clusters/clusterid1/Volumes/volumeid2":
                str = """{"action": "get",
                "node": {"key":
                "/clusters/clusterid1/Volumes/volumeid2",
                "dir": true,
                "nodes": [
                {"key":
                "/clusters/clusterid1/Volumes/volumeid2/status",
                "value": "Started",
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid2/vol_id",
                "value": "volumeid2",
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid2/vol_type",
                "value": "Distribute",
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid2/Bricks",
                "dir": true,
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid2/brick_count",
                "value": "3",
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid2/cluster_id",
                "value": "clusterid1",
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid2/deleted",
                "value": "",
                "modifiedIndex": 1,
                "createdIndex": 1
                },
                {"key":
                "/clusters/clusterid1/Volumes/volumeid2/name",
                "value": "vol2",
                "modifiedIndex": 1,
                "createdIndex": 1
                }
                ],
                "modifiedIndex": 1,
                "createdIndex": 1}}"""
                result = json.loads(str.decode('utf-8'))
                return etcd.EtcdResult(**result)

        monkeypatch.setattr(
            self.obj.client,
            'read',
            mock_etcd_read
        )

        self.obj.refresh()

        assert self.obj.name == "vol1"
        assert self.obj.vol_type == "Distribute"
        assert self.obj.brick_count == "3"
        assert self.obj.deleted == ""
        assert self.obj.status == "Started"
        assert not self.obj.__populate_brick_details__.called
