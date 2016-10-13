import json

import etcd
from tendrl.gluster_bridge.atoms.volume.bitrot_scrub_frequency import BitrotScrubFrequency


class BitrotScrubFrequency(object):
    def __init__(self, api_job):
        super(BitrotScrubFrequency, self).__init__()
        self.api_job = api_job
        self.atom = BitrotScrubFrequency

    def start(self):
        attributes = json.loads(self.api_job['attributes'].decode('utf-8'))
        vol_name = attributes['volname']
        frequency = attributes['frequency']
        self.atom().start(vol_name, frequency)
        self.api_job['status'] = "finished"
        etcd.Client().write(self.api_job['request_id'],
                            json.dumps(self.api_job))
