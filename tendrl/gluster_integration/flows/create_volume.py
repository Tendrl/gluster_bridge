import json

import etcd
from tendrl.gluster_integration.objects.volume.atoms.create import Create


class CreateVolume(object):
    def __init__(self, api_job):
        super(CreateVolume, self).__init__()
        self.api_job = api_job
        self.atom = Create

    def start(self):
        attributes = self.api_job['parameters']
        vol_name = attributes['volname']
        brickdetails = attributes['brickdetails']
        v_stripe_count = attributes.get('stripe_count')
        v_replica_count = attributes.get('replica_count')
        v_arbiter_count = attributes.get('arbiter_count')
        v_disperse_count = attributes.get('disperse_count')
        v_disperse_data_count = attributes.get('disperse_data_count')
        v_redundancy_count = attributes.get('redundancy_count')
        v_transport = attributes.get('transport')
        if v_stripe_count is not None:
            self.atom().start(
                vol_name,
                brickdetails,
                transport=v_transport,
                stripe_count=v_stripe_count
            )
        elif v_replica_count is not None:
            self.atom().start(
                vol_name,
                brickdetails,
                transport=v_transport,
                replica_count=v_replica_count,
                arbiter_count=v_arbiter_count
            )
        elif v_disperse_count is not None:
            self.atom().start(
                vol_name,
                brickdetails,
                transport=v_transport,
                disperse_count=v_disperse_count,
                disperse_data_count=v_disperse_data_count,
                redundancy_count=v_redundancy_count
            )
        else:
            self.atom().start(
                vol_name,
                brickdetails,
                transport=v_transport
            )
        self.api_job['status'] = "finished"
        etcd.Client().write(self.api_job['request_id'],
                            json.dumps(self.api_job))
