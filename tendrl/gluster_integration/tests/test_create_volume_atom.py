import subprocess

from mock import MagicMock
from tendrl.gluster_integration.objects.volume.atoms.create import Create


class TestCreateVolume(object):
    def test_run(self):
        subprocess.call = MagicMock(
            return_value='done'
        )

        parameters = {}
        parameters['Volume.volname'] = 'vol1'
        parameters['Volume.bricks'] = ['host1:brick1', 'host1:brick2']
        self.atom = Create(
            "create",
            True,
            "Create volume",
            ["Volume.volname", "Volume.bricks"],
            [],
            "242f6190-9b37-11e6-950d-a24fc0d9649c"
        )
        self.atom.run(parameters)

        subprocess.call.assert_called_with(
            [
                'gluster', 'volume', 'start',
                'vol1', '--mode=script'
            ]
        )
