import etcd

from tendrl.commons import objects
from tendrl.commons.utils import log_utils as logger
from tendrl.gluster_integration.objects.volume import Volume


class NamedVolumeNotExists(objects.BaseAtom):
    def __init__(self, *args, **kwargs):
        super(NamedVolumeNotExists, self).__init__(*args, **kwargs)

    def run(self):
        logger.log(
            "info",
            NS.publisher_id,
            {"message": "Checking if volume %s doesnt exist" %
             self.parameters['Volume.volname']},
            job_id=self.parameters["job_id"],
            flow_id=self.parameters["flow_id"],
            integration_id=NS.tendrl_context.integration_id,
        )
        try:
            volumes = NS._int.client.read(
                "clusters/%s/Volumes" % NS.tendrl_context.integration_id
            )
        except etcd.EtcdKeyNotFound:
            # No volumes present yet for cluster, return true
            return True

        for volume in volumes._children:
            fetched_volume = Volume(
                vol_id=volume['key'].split('/')[-1]
            ).load()
            if fetched_volume.name == \
                self.parameters['Volume.volname']:
                logger.log(
                    "warning",
                    NS.publisher_id,
                    {"message": "Volume %s already exists" %
                     self.parameters['Volume.volname']},
                    job_id=self.parameters["job_id"],
                    flow_id=self.parameters["flow_id"],
                    integration_id=NS.tendrl_context.integration_id
                )
                return False

        return True
