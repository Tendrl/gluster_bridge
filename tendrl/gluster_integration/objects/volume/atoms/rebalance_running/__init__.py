import etcd

from tendrl.commons import objects
from tendrl.commons.utils import log_utils as logger


class RebalanceRunning(objects.BaseAtom):
    def __init__(self, *args, **kwargs):
        super(RebalanceRunning, self).__init__(*args, **kwargs)

    def run(self):
        logger.log(
            "info",
            NS.publisher_id,
            {"message": "Checking if rebalance is running"},
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
                if rebal_status in ["in progress", "in_progress"]:
                    return True
                else:
                    logger.log(
                        "info",
                        NS.publisher_id,
                        {"message": "No rebalance running for"
                         " volume %s" % self.parameters[
                             'Volume.volname'
                         ]},
                        job_id=self.parameters["job_id"],
                        flow_id=self.parameters["flow_id"],
                        integration_id=NS.tendrl_context.integration_id
                    )
                    return False
            else:
                return False
        except etcd.EtcdKeyNotFound:
            logger.log(
                "info",
                NS.publisher_id,
                {"message": "Volume %s not found" %
                 self.parameters['Volume.volname']},
                job_id=self.parameters["job_id"],
                flow_id=self.parameters["flow_id"],
                integration_id=NS.tendrl_context.integration_id
            )
            return False
