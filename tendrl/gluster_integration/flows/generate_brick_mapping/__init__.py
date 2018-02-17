from tendrl.commons import flows
from tendrl.commons.utils import log_utils as logger


# This flow will be invoked only after cluster is available
# in the tendrl (either by creation/import)
class GenerateBrickMapping(flows.BaseFlow):
    def run(self):
        logger.log(
            "info",
            NS.publisher_id,
            {"message": "Starting flow for brick mapping generation"},
            job_id=self.job_id,
            flow_id=self.parameters['flow_id'],
            integration_id=NS.tendrl_context.integration_id
        )
        super(GenerateBrickMapping, self).run()
