from tendrl.commons import flows
from tendrl.commons.utils import log_utils as logger


class DeleteVolume(flows.BaseFlow):
    def __init__(self, *args, **kwargs):
        super(DeleteVolume, self).__init__(*args, **kwargs)

    def run(self):
        logger.log(
            "info",
            NS.publisher_id,
            {"message": "Starting deletion flow for volume %s" %
             self.parameters['Volume.volname']},
            job_id=self.parameters["job_id"],
            flow_id=self.parameters["flow_id"],
            integration_id=NS.tendrl_context.integration_id
        )
        super(DeleteVolume, self).run()
