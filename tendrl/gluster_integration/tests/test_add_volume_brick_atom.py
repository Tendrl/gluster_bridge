import mock
import subprocess

from tendrl.gluster_integration.objects.volume.atoms.add_brick import AddBrick


class TestAddVolumeBrick(object):

    def test_start(object):
        subprocess.call = mock.create_autospec(
            subprocess.call,
            return_value='done'
        )
        atom = AddBrick()
        atom.start("vol1", ['brick1', 'brick2'])
        subprocess.call.assert_called_with(
            [
                'gluster', 'volume', 'add-brick',
                'vol1', 'brick1', 'brick2',
                'force', '--mode=script'
            ]
        )
