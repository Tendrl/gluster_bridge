from mock import MagicMock
import sys

sys.modules[
    'tendrl.gluster_integration.objects.volume.atoms.replace_brick_commit'
] = MagicMock()
sys.modules['etcd'] = MagicMock()
from tendrl.gluster_bridge.flows.volume_replace_brick_commit \
    import VolumeReplaceBrickCommit


class TestVolumeReplaceBrickCommit(object):

    def test_start(object):
        flow = VolumeReplaceBrickCommit(
            {
                "request_id": "49fa2adde8a6e98591f0f5cb4bc5f44d",
                "sds_type": "gluster",
                "flow": "VolumeReplaceBrickCommit",
                "object_type": "volume",
                "status": 'new',
                "message": 'Starting replace brick for volume',
                "attributes": {
                    "volname": 'Volume1',
                    "source_brick": "brick1",
                    "destination_brick": "brick2"
                },
                "errors": {}
            })
        flow.start()
        flow.atom().start.assert_called_with('Volume1', 'brick1', 'brick2')
        assert flow.api_job['status'] == "finished"
