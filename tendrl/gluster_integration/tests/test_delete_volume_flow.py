from mock import MagicMock
import sys

sys.modules['tendrl.gluster_integration.objects.volume.atoms.delete'] = \
    MagicMock()
sys.modules['etcd'] = MagicMock()
from tendrl.gluster_integration.flows.delete_volume import DeleteVolume


class TestDeleteVolume(object):

    def test_start(object):
        flow = DeleteVolume(
            {
                "request_id": "49fa2adde8a6e98591f0f5cb4bc5f44d",
                "sds_type": "gluster",
                "flow": "DeleteVolume",
                "object_type": "volume",
                "status": 'new',
                "message": 'Deleting cluster',
                "parameters": {
                    "volname": 'Volume1',
                },
                "errors": {}
            })
        flow.start()
        flow.atom().start.assert_called_with('Volume1')
        assert flow.api_job['status'] == "finished"
