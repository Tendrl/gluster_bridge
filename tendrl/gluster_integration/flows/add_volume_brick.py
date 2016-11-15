import json

import etcd
from tendrl.gluster_integration.objects.volume.atoms.add_brick import AddBrick


class AddVolumeBrick(object):
    def __init__(self, api_job):
        super(AddVolumeBrick, self).__init__()
        self.api_job = api_job
        self.atom = AddBrick

    def start(self):
        attributes = self.api_job['parameters']
        vol_name = attributes['volname']
        bricks = attributes['brickdetails']
        self.atom().start(vol_name, bricks)
        self.api_job['status'] = "finished"
        etcd.Client().write(self.api_job['request_id'],
                            json.dumps(self.api_job))
