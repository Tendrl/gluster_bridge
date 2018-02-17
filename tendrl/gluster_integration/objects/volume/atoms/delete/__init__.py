import etcd

from tendrl.commons import objects
from tendrl.commons.utils import log_utils as logger


class Delete(objects.BaseAtom):
    def __init__(self, *args, **kwargs):
        super(Delete, self).__init__(*args, **kwargs)

    def run(self):
        if NS.gdeploy_plugin.stop_volume(
                self.parameters.get('Volume.volname')
        ):
            logger.log(
                "info",
                NS.publisher_id,
                {"message": "Stopped the volume %s before delete" %
                 self.parameters['Volume.volname']},
                job_id=self.parameters["job_id"],
                flow_id=self.parameters["flow_id"],
                integration_id=NS.tendrl_context.integration_id
            )
        else:
            logger.log(
                "error",
                NS.publisher_id,
                {"message": "Could not stop volume %s before delete" %
                 self.parameters['Volume.volname']},
                job_id=self.parameters["job_id"],
                flow_id=self.parameters["flow_id"],
                integration_id=NS.tendrl_context.integration_id
            )
            return False
        args = {}
        if self.parameters.get('Volume.volname') is not None:
            args.update({
                "format_bricks": self.parameters.get('Volume.format_bricks')
            })

        if NS.gdeploy_plugin.delete_volume(
                self.parameters.get('Volume.volname'),
                **args
        ):
            logger.log(
                "info",
                NS.publisher_id,
                {"message": "Deleted the volume %s" %
                 self.parameters['Volume.volname']},
                job_id=self.parameters["job_id"],
                flow_id=self.parameters["flow_id"],
                integration_id=NS.tendrl_context.integration_id
            )
        else:
            logger.log(
                "error",
                NS.publisher_id,
                {"message": "Failed to delete volume %s" %
                 self.parameters['Volume.volname']},
                job_id=self.parameters["job_id"],
                flow_id=self.parameters["flow_id"],
                integration_id=NS.tendrl_context.integration_id
            )
            return False

        while True:
            try:
                NS._int.wclient.delete(
                    "clusters/%s/Volumes/%s" % (
                        NS.tendrl_context.integration_id,
                        self.parameters['Volume.vol_id']
                    ),
                    recursive=True
                )
            except (etcd.EtcdKeyNotFound, KeyError):
                logger.log(
                    "info",
                    NS.publisher_id,
                    {"message": "Deleted the volume %s" %
                     self.parameters['Volume.volname']},
                    job_id=self.parameters["job_id"],
                    flow_id=self.parameters["flow_id"],
                    integration_id=NS.tendrl_context.integration_id
                )
            finally:
                return True
