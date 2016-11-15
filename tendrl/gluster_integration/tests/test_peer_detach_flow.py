from mock import MagicMock
import sys

sys.modules['tendrl.gluster_integration.objects.peer.atoms.detach'] = \
    MagicMock()
sys.modules['etcd'] = MagicMock()
from tendrl.gluster_integration.flows.peer_detach import PeerDetach


class TestPeerDetach(object):

    def test_start(object):
        flow = PeerDetach(
            {
                "request_id": "49fa2adde8a6e98591f0f5cb4bc5f44d",
                "sds_type": "gluster",
                "flow": "PeerDetach",
                "object_type": "peer",
                "status": 'new',
                "message": 'Detaching a peer',
                "parameters": {
                    "peer": 'node1',
                },
                "errors": {}
            })
        flow.start()
        flow.atom().start.assert_called_with('node1')
        assert flow.api_job['status'] == "finished"
