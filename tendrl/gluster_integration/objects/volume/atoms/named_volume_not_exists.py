import logging

from tendrl.gluster_integration.objects.volume import volume

LOG = logging.getLogger(__name__)


class NamedVolumeNotExists(object):
    def run(self, parameters):
        vol = volume.Volume(
            parameters.get("Volume.volname"),
            "",
            parameters.get("Tendrl_context.cluster_id")
        )
        vol.refresh()

        if vol.vol_type is None or vol.vol_type == "":
            return True

        LOG.error(
            "Volume: %s already exists for cluster: %s" %
            (
                parameters.get("Volume.volname"),
                parameters.get("Tendrl_context.cluster_id")
            )
        )
        return False
