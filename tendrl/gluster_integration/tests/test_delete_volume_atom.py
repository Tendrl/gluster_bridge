import mock
import subprocess

from tendrl.gluster_integration.objects.volume.atoms.delete import Delete


class TestDeleteVolume(object):

    def test_start(object):
        subprocess.call = mock.create_autospec(
            subprocess.call,
            return_value='done'
        )
        atom = Delete()
        atom.start("vol1")
        subprocess.call.assert_called_with(
            [
                'gluster', 'volume', 'delete',
                'vol1', '--mode=script'
            ]
        )
