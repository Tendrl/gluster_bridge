from tendrl.commons import objects
from tendrl.commons.utils import log_utils as logger


class Stop(objects.BaseAtom):
    def __init__(self, *args, **kwargs):
        super(Stop, self).__init__(*args, **kwargs)

    def run(self):
        if NS.gdeploy_plugin.stop_volume(
            self.parameters.get('Volume.volname')
        ):
            logger.log(
                "info",
                NS.publisher_id,
                {"message": "Stopped the volume %s successfully" %
                 self.parameters['Volume.volname']},
                job_id=self.parameters["job_id"],
                flow_id=self.parameters["flow_id"],
                integration_id=NS.tendrl_context.integration_id
            )
        else:
            logger.log(
                "error",
                NS.publisher_id,
                {"message": "Failed to stop the volume %s" %
                 self.parameters['Volume.volname']},
                job_id=self.parameters["job_id"],
                flow_id=self.parameters["flow_id"],
                integration_id=NS.tendrl_context.integration_id
            )
            return False
        return True
