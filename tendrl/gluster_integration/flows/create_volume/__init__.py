from tendrl.commons import flows
from tendrl.commons.utils import log_utils as logger


class CreateVolume(flows.BaseFlow):
    def run(self):
        logger.log(
            "info",
            NS.publisher_id,
            {"message": "Starting creation flow for volume %s" %
             self.parameters['Volume.volname']},
            job_id=self.job_id,
            flow_id=self.parameters['flow_id'],
            integration_id=NS.tendrl_context.integration_id
        )
        super(CreateVolume, self).run()
