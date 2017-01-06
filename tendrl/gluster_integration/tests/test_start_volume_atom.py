import subprocess

from mock import MagicMock
from tendrl.gluster_integration.objects.volume.atoms.start import Start


class TestStartVolume(object):
    def test_run(self):
        subprocess.call = MagicMock(
            return_value='done'
        )

        parameters = {}
        parameters['Volume.volname'] = 'vol1'
        self.atom = Start(
            "start",
            True,
            "Start volume",
            ["Volume.vol_id"],
            [],
            "242f6190-9b37-11e6-950d-a24fc0d9651c"
        )
        self.atom.run(parameters)

        subprocess.call.assert_called_with(
            [
                'gluster', 'volume', 'start',
                'vol1', '--mode=script'
            ]
        )
