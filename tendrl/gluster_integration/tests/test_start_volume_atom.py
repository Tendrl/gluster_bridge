import mock
import subprocess

from tendrl.gluster_integration.objects.volume.atoms.start import Start


class TestStartVolume(object):

    def test_start(object):
        subprocess.call = mock.create_autospec(
            subprocess.call,
            return_value='done'
        )
        atom = Start()
        atom.start("vol1")
        subprocess.call.assert_called_with(
            [
                'gluster', 'volume', 'start',
                'vol1', '--mode=script'
            ]
        )
