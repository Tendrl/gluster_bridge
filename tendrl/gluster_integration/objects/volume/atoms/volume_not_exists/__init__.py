import etcd

from tendrl.commons import objects
from tendrl.commons.utils import log_utils as logger


class VolumeNotExists(objects.BaseAtom):
    def __init__(self, *args, **kwargs):
        super(VolumeNotExists, self).__init__(*args, **kwargs)

    def run(self):
        logger.log(
            "info",
            NS.publisher_id,
            {"message": "Checking if volume %s doesnt exist" %
             self.parameters['Volume.volname']},
            job_id=self.parameters["job_id"],
            flow_id=self.parameters["flow_id"],
            integration_id=NS.tendrl_context.integration_id
        )
        try:
            NS._int.client.read(
                'clusters/%s/Volumes/%s' % (
                    NS.tendrl_context.integration_id,
                    self.parameters['Volume.vol_id']
                )
            )
        except etcd.EtcdKeyNotFound:
            logger.log(
                "warning",
                NS.publisher_id,
                {"message": "Volume %s doesnt exist" %
                 self.parameters['Volume.volname']},
                job_id=self.parameters["job_id"],
                flow_id=self.parameters["flow_id"],
                integration_id=NS.tendrl_context.integration_id
            )
            return True

        return False
