import etcd

from tendrl.commons import objects
from tendrl.commons.utils import log_utils as logger


class RebalanceNotRunning(objects.BaseAtom):
    def __init__(self, *args, **kwargs):
        super(RebalanceNotRunning, self).__init__(*args, **kwargs)

    def run(self):
        logger.log(
            "info",
            NS.publisher_id,
            {"message": "Checking if rebalance is not running"},
            job_id=self.parameters["job_id"],
            flow_id=self.parameters["flow_id"],
            integration_id=NS.tendrl_context.integration_id
        )
        try:
            rebal_status = NS._int.client.read(
                'clusters/%s/Volumes/%s/rebal_status' % (
                    NS.tendrl_context.integration_id,
                    self.parameters['Volume.vol_id']
                )
            ).value
            if rebal_status is not None:
                if rebal_status == "not applicable" or\
                    rebal_status == "not_started" or\
                    rebal_status == "completed":
                    return True
                if rebal_status == "in progress":
                    return False
                logger.log(
                    "info",
                    NS.publisher_id,
                    {"message": "Volume rebalance status is %s" %
                     rebal_status},
                    job_id=self.parameters["job_id"],
                    flow_id=self.parameters["flow_id"],
                    integration_id=NS.tendrl_context.integration_id
                )
                return False
            else:
                return True
        except etcd.EtcdKeyNotFound:
            logger.log(
                "error",
                NS.publisher_id,
                {"message": "Volume %s not found" %
                 self.parameters['Volume.volname']},
                job_id=self.parameters["job_id"],
                flow_id=self.parameters["flow_id"],
                integration_id=NS.tendrl_context.integration_id
            )
            return False
