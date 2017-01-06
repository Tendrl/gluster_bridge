import logging

from tendrl.gluster_integration.objects.volume import volume

LOG = logging.getLogger(__name__)


class NamedVolumeExists(object):
    def __init__(self):
        self.vol = None

    def run(self, parameters):
        self.vol = volume.Volume(
            parameters.get("Volume.volname"),
            "",
            parameters.get("Tendrl_context.cluster_id")
        )
        self.vol.refresh()

        if self.vol.deleted == "" or self.vol.deleted == "False":
            return True

        LOG.error(
            "Volume: %s does not exist for cluster: %s" %
            (
                parameters.get("Volume.volname"),
                parameters.get("Tendrl_context.cluster_id")
            )
        )
        return False
