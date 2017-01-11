import subprocess

from tendrl.commons.atoms.base_atom import BaseAtom


class Create(BaseAtom):
    def run(self, parameters):
        cmd = ['gluster', 'volume', 'create', parameters.get('Volume.volname')]
        if parameters.get('Volume.stripe_count') is not None:
            cmd.append('stripe')
            cmd.append(str(parameters.get('Volume.stripe_count')))
        elif parameters.get('Volume.replica_count') is not None:
            cmd.append('replica')
            cmd.append(str(parameters.get('Volume.replica_count')))
            if parameters.get('Volume.arbiter_count') is not None:
                cmd.append('arbiter')
                cmd.append(str(parameters.get('Volume.arbiter_count')))
        elif parameters.get('Volume.disperse_count') is not None:
            cmd.append('disperse')
            cmd.append(str(parameters.get('Volume.disperse_count')))
        elif parameters.get('Volume.redundancy_count') is not None:
            cmd.append('redundancy')
            cmd.append(str(parameters.get('Volume.redundancy_count')))
        elif parameters.get('Volume.disperse_data_count') is not None:
            cmd.append('disperse-data')
            cmd.append(str(parameters.get('Volume.disperse_data_count')))
        if parameters.get('Volume.transport'):
            cmd.append('transport')
            cmd.append(','.join(parameters.get('Volume.transport')))
        cmd.extend(parameters.get('Volume.bricks'))
        cmd.append('force')
        cmd.append('--mode=script')
        subprocess.call(cmd)
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
