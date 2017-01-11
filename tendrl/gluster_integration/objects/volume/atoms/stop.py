import subprocess

from tendrl.commons.atoms.base_atom import BaseAtom


class Stop(BaseAtom):
    def run(self, parameters):
        subprocess.call(
            [
                'gluster',
                'volume',
                'stop',
                parameters.get('Volume.volname'),
                '--mode=script'
            ]
        )
        return True
