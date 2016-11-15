import mock
import subprocess

from tendrl.gluster_integration.objects.peer.atoms.probe import Probe


class TestPeerProbe(object):

    def test_start(object):
        subprocess.call = mock.create_autospec(
            subprocess.call,
            return_value='done'
        )
        atom = Probe()
        atom.start("node1")
        subprocess.call.assert_called_with(
            [
                'gluster', 'peer', 'probe',
                'node1', '--mode=script'
            ]
        )
