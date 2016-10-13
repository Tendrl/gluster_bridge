import json

import etcd
from tendrl.gluster_bridge.atoms.volume.replace_brick_abort import ReplaceBrickAbort


class ReplaceBrickAbort(object):
    def __init__(self, api_job):
        super(ReplaceBrickAbort, self).__init__()
        self.api_job = api_job
        self.atom = ReplaceBrickAbort

    def start(self):
        attributes = json.loads(self.api_job['attributes'].decode('utf-8'))
        vol_name = attributes['volname']
        brick = attributes['source_brick']
        new_brick = attributes['destination_brick']
        self.atom().start(vol_name, brick, new_brick)
        self.api_job['status'] = "finished"
        etcd.Client().write(self.api_job['request_id'],
                            json.dumps(self.api_job))
