import subprocess

from tendrl.commons.atoms.base_atom import BaseAtom


class Start(BaseAtom):
    def run(self, parameters):
        subprocess.call(
            [
                'gluster',
                'volume',
                'start',
                parameters.get('Volume.volname'),
                '--mode=script'
            ]
        )
        return True
