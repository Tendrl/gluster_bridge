import json

import etcd
from tendrl.gluster_bridge.atoms.volume.add_brick import AddBrick


class AddBrick(object):
    def __init__(self, api_job):
        super(AddBrick, self).__init__()
        self.api_job = api_job
        self.atom = AddBrick

    def start(self):
        attributes = json.loads(self.api_job['attributes'].decode('utf-8'))
        vol_name = attributes['volname']
        brick = attributes['brick_name']
        self.atom().start(vol_name, brick)
        self.api_job['status'] = "finished"
        etcd.Client().write(self.api_job['request_id'],
                            json.dumps(self.api_job))
