import subprocess

from tendrl.commons.atoms.base_atom import BaseAtom


class Create(BaseAtom):
    def run(self):
        cmd = [
            'gluster',
            'volume',
            'create',
            self.parameters.get('Volume.volname')
        ]
        if self.parameters.get('Volume.stripe_count') is not None:
            cmd.append('stripe')
            cmd.append(str(self.parameters.get('Volume.stripe_count')))
        elif self.parameters.get('Volume.replica_count') is not None:
            cmd.append('replica')
            cmd.append(str(self.parameters.get('Volume.replica_count')))
            if self.parameters.get('Volume.arbiter_count') is not None:
                cmd.append('arbiter')
                cmd.append(str(self.parameters.get('Volume.arbiter_count')))
        elif self.parameters.get('Volume.disperse_count') is not None:
            cmd.append('disperse')
            cmd.append(str(self.parameters.get('Volume.disperse_count')))
        elif self.parameters.get('Volume.redundancy_count') is not None:
            cmd.append('redundancy')
            cmd.append(str(self.parameters.get('Volume.redundancy_count')))
        elif self.parameters.get('Volume.disperse_data_count') is not None:
            cmd.append('disperse-data')
            cmd.append(str(self.parameters.get('Volume.disperse_data_count')))
        if self.parameters.get('Volume.transport'):
            cmd.append('transport')
            cmd.append(','.join(self.parameters.get('Volume.transport')))
        cmd.extend(self.parameters.get('Volume.bricks'))
        cmd.append('force')
        cmd.append('--mode=script')
        subprocess.call(cmd)
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
