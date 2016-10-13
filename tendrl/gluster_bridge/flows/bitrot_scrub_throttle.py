import json

import etcd
from tendrl.gluster_bridge.atoms.volume.bitrot_scrub_throttle import BitrotScrubThrottle


class BitrotScrubThrottle(object):
    def __init__(self, api_job):
        super(BitrotScrubThrottle, self).__init__()
        self.api_job = api_job
        self.atom = BitrotScrubThrottle

    def start(self):
        attributes = json.loads(self.api_job['attributes'].decode('utf-8'))
        vol_name = attributes['volname']
        mode = attributes['mode']
        self.atom().start(vol_name, mode)
        self.api_job['status'] = "finished"
        etcd.Client().write(self.api_job['request_id'],
                            json.dumps(self.api_job))
