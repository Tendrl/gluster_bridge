import json

import etcd
from tendrl.gluster_bridge.atoms.volume.rebalance_start import RebalanceStart


class RebalanceStart(object):
    def __init__(self, api_job):
        super(RebalanceStart, self).__init__()
        self.api_job = api_job
        self.atom = RebalanceStart

    def start(self):
        attributes = json.loads(self.api_job['attributes'].decode('utf-8'))
        vol_name = attributes['volname']
        fix_layout = attributes['fix_layout']
        force = attributes['force']
        self.atom().start(vol_name, force, fix_layout)
        self.api_job['status'] = "finished"
        etcd.Client().write(self.api_job['request_id'],
                            json.dumps(self.api_job))
