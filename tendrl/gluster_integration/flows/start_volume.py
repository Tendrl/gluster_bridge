import json

import etcd
from tendrl.gluster_integration.objects.volume.atoms.start import Start


class StartVolume(object):
    def __init__(self, api_job):
        super(StartVolume, self).__init__()
        self.api_job = api_job
        self.atom = Start

    def start(self):
        attributes = self.api_job['parameters']
        vol_name = attributes['volname']
        self.atom().start(vol_name)
        self.api_job['status'] = "finished"
        etcd.Client().write(self.api_job['request_id'],
                            json.dumps(self.api_job))
