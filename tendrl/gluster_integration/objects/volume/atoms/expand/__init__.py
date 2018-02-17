from tendrl.commons import objects
from tendrl.commons.utils import log_utils as logger
from tendrl.gluster_integration.objects.volume import Volume


class Expand(objects.BaseAtom):
    def __init__(self, *args, **kwargs):
        super(Expand, self).__init__(*args, **kwargs)

    def run(self):
        args = {}
        vol = Volume(vol_id=self.parameters['Volume.vol_id']).load()
        if self.parameters.get('Volume.replica_count') is not None:
            args.update({
                "replica_count": self.parameters.get('Volume.replica_count')
            })
            vol = Volume(vol_id=self.parameters['Volume.vol_id']).load()
            if vol.replica_count != self.parameters.get(
                    'Volume.replica_count'
            ):
                args.update({"increase_replica_count": True})
        elif self.parameters.get('Volume.disperse_count') is not None:
            args.update({
                "disperse_count": self.parameters.get('Volume.disperse_count')
            })
        else:
            if int(vol.replica_count) > 1:
                args.update({
                    "replica_count": vol.replica_count
                })
            elif int(vol.disperse_count) > 1:
                args.update({
                    "disperse_count": vol.disperse_count
                })

        if self.parameters.get('Volume.force') is not None:
            args.update({
                "force": self.parameters.get('Volume.force')
            })

        logger.log(
            "info",
            NS.publisher_id,
            {"message": "Expanding the volume %s" %
             self.parameters['Volume.volname']},
            job_id=self.parameters["job_id"],
            flow_id=self.parameters["flow_id"],
            integration_id=NS.tendrl_context.integration_id
        )
        if NS.gdeploy_plugin.expand_volume(
                self.parameters.get('Volume.volname'),
                self.parameters.get('Volume.bricks'),
                **args
        ):
            logger.log(
                "info",
                NS.publisher_id,
                {"message": "Expanded the volume %s" %
                 self.parameters['Volume.volname']},
                job_id=self.parameters["job_id"],
                flow_id=self.parameters["flow_id"],
                integration_id=NS.tendrl_context.integration_id
            )
            return True
        else:
            logger.log(
                "error",
                NS.publisher_id,
                {"message": "Volume expansion failed for volume %s" %
                 self.parameters['Volume.volname']},
                job_id=self.parameters["job_id"],
                flow_id=self.parameters["flow_id"],
                integration_id=NS.tendrl_context.integration_id
            )
            return False
