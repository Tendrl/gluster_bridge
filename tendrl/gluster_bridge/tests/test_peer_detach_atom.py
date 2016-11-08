import mock
import subprocess

from tendrl.gluster_bridge.atoms.peer.detach import Detach


class TestPeerDetach(object):

    def test_start(object):
        subprocess.call = mock.create_autospec(
            subprocess.call,
            return_value='done'
        )
        atom = Detach()
        atom.start("node1")
        subprocess.call.assert_called_with(
            [
                'gluster', 'peer', 'detach',
                'node1', '--mode=script'
            ]
        )