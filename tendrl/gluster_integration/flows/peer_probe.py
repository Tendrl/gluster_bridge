import json

import etcd
from tendrl.gluster_integration.objects.peer.atoms.probe import Probe


class PeerProbe(object):
    def __init__(self, api_job):
        super(PeerProbe, self).__init__()
        self.api_job = api_job
        self.atom = Probe

    def start(self):
        attributes = self.api_job['parameters']
        peer = attributes['peer']
        self.atom().start(peer)
        self.api_job['status'] = "finished"
        etcd.Client().write(self.api_job['request_id'],
                            json.dumps(self.api_job))
