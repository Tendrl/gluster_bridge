import subprocess

from mock import MagicMock
from tendrl.gluster_integration.objects.volume.atoms.stop import Stop


class TestStopVolume(object):
    def test_run(self):
        subprocess.call = MagicMock(
            return_value='done'
        )

        parameters = {}
        parameters['Volume.volname'] = 'vol1'
        self.atom = Stop(
            "stop",
            True,
            "Stop volume",
            ["Volume.vol_id"],
            [],
            "242f6190-9b37-11e6-950d-a24fc0d9652c"
        )
        self.atom.run(parameters)

        subprocess.call.assert_called_with(
            [
                'gluster', 'volume', 'stop',
                'vol1', '--mode=script'
            ]
        )
