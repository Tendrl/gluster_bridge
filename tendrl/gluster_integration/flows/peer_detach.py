import json
import socket

import etcd
from tendrl.gluster_integration.objects.peer.atoms.detach import Detach


class PeerDetach(object):
    def __init__(self, api_job):
        super(PeerDetach, self).__init__()
        self.api_job = api_job
        self.atom = Detach

    def start(self):
        attributes = self.api_job['parameters']
        peer = attributes['peer']
        # If host is trying to detach itself, dont allow
        if socket.gethostname() == peer:
            return
        self.atom().start(peer)
        self.api_job['status'] = "finished"
        etcd.Client().write(self.api_job['request_id'],
                            json.dumps(self.api_job))
