import subprocess

from tendrl.commons.atoms.base_atom import BaseAtom


class Start(BaseAtom):
    def run(self):
        subprocess.call(
            [
                'gluster',
                'volume',
                'start',
                self.parameters.get('Volume.volname'),
                '--mode=script'
            ]
        )
        return True
