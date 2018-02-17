import time

import etcd

from tendrl.commons import objects
from tendrl.commons.objects import AtomExecutionFailedError
from tendrl.commons.utils import log_utils as logger
from tendrl.gluster_integration.objects.volume import Volume


class CheckVolumeAvailable(objects.BaseAtom):
    def __init__(self, *args, **kwargs):
        super(CheckVolumeAvailable, self).__init__(*args, **kwargs)

    def run(self):
        retry_count = 0
        while True:
            volumes = None
            try:
                volumes = NS._int.client.read(
                    "clusters/%s/Volumes" % NS.tendrl_context.integration_id
                )
            except etcd.EtcdKeyNotFound:
                # ignore as no volumes available till now
                pass

            if volumes:
                for entry in volumes.leaves:
                    volume = Volume(
                        vol_id=entry.key.split("Volumes/")[-1]
                    ).load()
                    if volume.name == self.parameters['Volume.volname']:
                        return True

            retry_count += 1
            time.sleep(1)
            if retry_count == 600:
                logger.log(
                    "error",
                    NS.publisher_id,
                    {"message": "Volume %s not reflected in tendrl"
                     " yet. Timing out" % self.parameters[
                         'Volume.volname']},
                    job_id=self.parameters['job_id'],
                    flow_id=self.parameters['flow_id'],
                    integration_id=NS.tendrl_context.integration_id
                )
                raise AtomExecutionFailedError(
                    "Volume %s not reflected in tendrl yet. Timing out" %
                    self.parameters['Volume.volname']
                )
