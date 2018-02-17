from tendrl.commons import objects
from tendrl.commons.utils import log_utils as logger


class StartRebalance(objects.BaseAtom):
    def __init__(self, *args, **kwargs):
        super(StartRebalance, self).__init__(*args, **kwargs)

    def run(self):
        if NS.gdeploy_plugin.rebalance_volume(
                self.parameters.get('Volume.volname'),
                "start",
                force=self.parameters.get('Volume.force'),
                fix_layout=self.parameters.get('Volume.fix_layout')
        ):
            logger.log(
                "info",
                NS.publisher_id,
                {"message": "Started the rebalance for volume %s" %
                 self.parameters['Volume.volname']},
                job_id=self.parameters["job_id"],
                flow_id=self.parameters["flow_id"],
                integration_id=NS.tendrl_context.integration_id
            )
        else:
            logger.log(
                "error",
                NS.publisher_id,
                {"message": "Failed to start rebalance for volume %s" %
                 self.parameters['Volume.volname']},
                job_id=self.parameters["job_id"],
                flow_id=self.parameters["flow_id"],
                integration_id=NS.tendrl_context.integration_id
            )
            return False

        return True
