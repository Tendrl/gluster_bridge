import json

import etcd
from tendrl.gluster_bridge.atoms.volume.set import Set


class SetVolumeOption(object):
    def __init__(self, api_job):
        super(SetVolumeOption, self).__init__()
        self.api_job = api_job
        self.atom = SetVolumeOption

    def start(self):
        attributes = json.loads(self.api_job['attributes'].decode('utf-8'))
        vol_name = attributes['volname']
        option = attributes['option_name']
        option_value = attributes['option_value']
        self.atom().start(vol_name, option, option_value)
        self.api_job['status'] = "finished"
        etcd.Client().write(self.api_job['request_id'],
                            json.dumps(self.api_job))
