import logging

from tendrl.common.atoms.base_atom import BaseAtom
from tendrl.gluster_integration.objects.volume import volume

LOG = logging.getLogger(__name__)


class VolumeStopped(BaseAtom):
    def run(self, parameters):
        vol = volume.Volume(
            "",
            parameters.get("Volume.vol_id"),
            parameters.get("Tendrl_context.cluster_id")
        )
        vol.refresh()

        if vol.status == "Stopped":
            return True
        else:
            LOG.error(
                "Volume: %s not stopped for cluster: %s" %
                (
                    parameters.get("Volume.volname"),
                    parameters.get("Tendrl_context.cluster_id")
                )
            )
            return False
