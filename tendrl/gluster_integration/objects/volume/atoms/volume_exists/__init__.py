from tendrl.commons import objects
from tendrl.commons.utils import log_utils as logger
from tendrl.gluster_integration.objects.volume import Volume


class VolumeExists(objects.BaseAtom):
    def __init__(self, *args, **kwargs):
        super(VolumeExists, self).__init__(*args, **kwargs)

    def run(self):
        logger.log(
            "info",
            NS.publisher_id,
            {"message": "Checking if volume %s exists" %
             self.parameters['Volume.volname']},
            job_id=self.parameters["job_id"],
            flow_id=self.parameters["flow_id"],
            integration_id=NS.tendrl_context.integration_id
        )
        if Volume(vol_id=self.parameters['Volume.vol_id']).exists():
            return True
        else:
            logger.log(
                "error",
                NS.publisher_id,
                {"message": "Volume %s doesnt exist" %
                 self.parameters["Volume.volname"]},
                job_id=self.parameters["job_id"],
                flow_id=self.parameters["flow_id"],
                integration_id=NS.tendrl_context.integration_id
            )
            return False
