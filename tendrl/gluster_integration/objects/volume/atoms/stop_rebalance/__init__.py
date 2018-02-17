from tendrl.commons import objects
from tendrl.commons.utils import log_utils as logger


class StopRebalance(objects.BaseAtom):
    def __init__(self, *args, **kwargs):
        super(StopRebalance, self).__init__(*args, **kwargs)

    def run(self):
        if NS.gdeploy_plugin.rebalance_volume(
                self.parameters.get('Volume.volname'),
                "stop",
                force=self.parameters.get('Volume.force')
        ):
            logger.log(
                "info",
                NS.publisher_id,
                {"message": "Stopped the rebalance for volume %s" %
                 self.parameters['Volume.volname']},
                job_id=self.parameters["job_id"],
                flow_id=self.parameters["flow_id"],
                integration_id=NS.tendrl_context.integration_id
            )
        else:
            logger.log(
                "error",
                NS.publisher_id,
                {"message": "Failed to stop rebalance for volume %s" %
                 self.parameters['Volume.volname']},
                job_id=self.parameters["job_id"],
                flow_id=self.parameters["flow_id"],
                integration_id=NS.tendrl_context.integration_id
            )
            return False

        return True
