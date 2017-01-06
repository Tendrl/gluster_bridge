import etcd
import json

from mock import MagicMock

from tendrl.gluster_integration.objects.volume.atoms \
    import named_volume_not_exists
from tendrl.gluster_integration.objects.volume import volume


class TestNamedVolumeNotExists(object):
    def test_run(self, monkeypatch):
        self.obj = named_volume_not_exists.NamedVolumeNotExists()
        named_volume_not_exists.volume.etcd = MagicMock()
        named_volume_not_exists.volume.TendrlConfig = MagicMock()

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

        volume_obj = volume.Volume("vol1", "", "clusterid1")
        monkeypatch.setattr(
            volume_obj.client,
            'read',
            mock_etcd_read
        )

        parameters = {}
        parameters['Volume.volname'] = "vol2"
        parameters["Tendrl_context.cluster_id"] = "clusterid1"
        ret_val = self.obj.run(parameters)
        assert ret_val is True

        parameters['Volume.volname'] = "vol1"
        parameters["Tendrl_context.cluster_id"] = "clusterid1"
        ret_val = self.obj.run(parameters)
        assert ret_val is not True
