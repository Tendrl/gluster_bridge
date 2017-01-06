import etcd
import json

from mock import MagicMock

from tendrl.gluster_integration.objects.volume.atoms import volume_started


class TestVolumeStarted(object):
    def test_run(self, monkeypatch):
        self.obj = volume_started.VolumeStarted(
            "Start",
            True,
            "Start Volume",
            ["Volume.volname"],
            [],
            "1951e821-7aa9-4a91-8183-e73bc8275b5e"
        )
        volume_started.volume.etcd = MagicMock()
        volume_started.volume.TendrlConfig = MagicMock()
        volume_started.volume.brick.etcd = MagicMock()
        volume_started.volume.brick.TendrlConfig = MagicMock()
        volume_ins = volume_started.volume.Volume(
            "",
            "volumeid1",
            "clusterid1"
        )
        brick_ins = volume_started.volume.brick.Brick(
            "clusterid1",
            "volumeid1",
            "host1:brick1"
        )

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
                "value": "1",
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

        monkeypatch.setattr(
            volume_ins.client,
            'read',
            mock_etcd_read
        )
        monkeypatch.setattr(
            brick_ins.client,
            'read',
            mock_etcd_read
        )

        parameters = {}
        parameters['Volume.vol_id'] = "volumeid1"
        parameters["Tendrl_context.cluster_id"] = "clusterid1"
        ret_val = self.obj.run(parameters)
        assert ret_val is True

        parameters['Volume.vol_id'] = "xyz"
        parameters["Tendrl_context.cluster_id"] = "clusterid1"
        ret_val = self.obj.run(parameters)
        assert ret_val is not True
