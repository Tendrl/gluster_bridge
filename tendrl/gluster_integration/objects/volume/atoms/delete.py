import subprocess

from tendrl.commons.atoms.base_atom import BaseAtom


class Delete(BaseAtom):
    def run(self, parameters):
        cluster_id = parameters['Tendrl_context.cluster_id']
        vol_id = parameters['Volume.vol_id']
        subprocess.call(
            [
                'gluster',
                'volume',
                'stop',
                parameters.get('Volume.volname'),
                '--mode=script'
            ]
        )
        subprocess.call(
            [
                'gluster',
                'volume',
                'delete',
                parameters.get('Volume.volname'),
                '--mode=script'
            ]
        )
        etcd_client = parameters['etcd_client']
        vol_key = "clusters/%s/Volumes/%s/deleted" % (cluster_id, vol_id)
        etcd_client.write(vol_key, "True")
        return True
