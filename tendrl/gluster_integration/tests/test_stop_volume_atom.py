import mock
import subprocess

from tendrl.gluster_integration.objects.volume.atoms.stop import Stop


class TestStopVolume(object):

    def test_start(object):
        subprocess.call = mock.create_autospec(
            subprocess.call,
            return_value='done'
        )
        atom = Stop()
        atom.start("vol1")
        subprocess.call.assert_called_with(
            [
                'gluster', 'volume', 'stop',
                'vol1', '--mode=script'
            ]
        )
