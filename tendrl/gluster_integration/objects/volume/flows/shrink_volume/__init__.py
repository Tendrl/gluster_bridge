from tendrl.commons import flows
from tendrl.commons.utils import log_utils as logger


class ShrinkVolume(flows.BaseFlow):
    def __init__(self, *args, **kwargs):
        super(ShrinkVolume, self).__init__(*args, **kwargs)

    def run(self):
        logger.log(
            "info",
            NS.publisher_id,
            {"message": "Starting shrink volume flow for volume %s" %
             self.parameters['Volume.volname']},
            job_id=self.parameters["job_id"],
            flow_id=self.parameters["flow_id"],
            integration_id=NS.tendrl_context.integration_id
        )
        super(ShrinkVolume, self).run()
