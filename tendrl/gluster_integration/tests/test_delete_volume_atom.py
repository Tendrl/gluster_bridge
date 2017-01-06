import etcd
import subprocess

from mock import MagicMock
from tendrl.gluster_integration.objects.volume.atoms.delete import Delete


class TestDeleteVolume(object):
    def test_run(self):
        subprocess.call = MagicMock(
            return_value='done'
        )

        parameters = {}
        parameters['Tendrl_context.cluster_id'] = 'clusterid1'
        parameters['Volume.vol_id'] = 'volumeid1'
        parameters['Volume.volname'] = 'vol1'

        self.etcd_client = MagicMock(
            return_value=etcd.Client(
                **{'host': 'dummyhost', 'port': 2379}
            )
        )
        parameters['etcd_client'] = self.etcd_client
        self.atom = Delete(
            "delete",
            True,
            "Delete volume",
            ["Volume.vol_id"],
            [],
            "242f6190-9b37-11e6-950d-a24fc0d9650c"
        )
        self.atom.run(parameters)

        subprocess.call.assert_called_with(
            [
                'gluster', 'volume', 'delete',
                'vol1', '--mode=script'
            ]
        )

        self.etcd_client.write.assert_called_with(
            'clusters/clusterid1/Volumes/volumeid1/deleted',
            'True'
        )
