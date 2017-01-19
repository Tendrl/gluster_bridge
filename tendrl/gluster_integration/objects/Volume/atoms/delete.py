import subprocess

from tendrl.commons.atoms.base_atom import BaseAtom


class Delete(BaseAtom):
    def run(self):
        cluster_id = self.parameters['Tendrl_context.cluster_id']
        vol_id = self.parameters['Volume.vol_id']
        subprocess.call(
            [
                'gluster',
                'volume',
                'stop',
                self.parameters.get('Volume.volname'),
                '--mode=script'
            ]
        )
        subprocess.call(
            [
                'gluster',
                'volume',
                'delete',
                self.parameters.get('Volume.volname'),
                '--mode=script'
            ]
        )
        etcd_client = self.parameters["etcd_orm"].client
        vol_key = "clusters/%s/Volumes/%s/deleted" % (cluster_id, vol_id)
        etcd_client.write(vol_key, "True")
        return True
