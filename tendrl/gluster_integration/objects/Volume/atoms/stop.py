import subprocess

from tendrl.commons.atoms.base_atom import BaseAtom


class Stop(BaseAtom):
    def run(self):
        subprocess.call(
            [
                'gluster',
                'volume',
                'stop',
                self.parameters.get('Volume.volname'),
                '--mode=script'
            ]
        )
        return True
