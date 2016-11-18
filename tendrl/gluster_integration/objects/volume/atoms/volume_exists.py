import logging

from tendrl.common.atoms.base_atom import BaseAtom

LOG = logging.getLogger(__name__)


class VolumeExists(BaseAtom):
    def run(self, parameters):
        path = "/clusters/%s/Volumes/%s" %\
            (
                parameters.get("Tendrl_context.cluster_id"),
                parameters.get("Volume.vol_id")
            )
        etcd_client = parameters['etcd_client']
        volume = etcd_client.read(path)
        if volume is not None:
            return True
        else:
            LOG.error(
                "Volume: %s does not exist for cluster: %s" %
                (
                    parameters.get("Volume.volname"),
                    parameters.get("Tendrl_context.cluster_id")
                )
            )
            return False
