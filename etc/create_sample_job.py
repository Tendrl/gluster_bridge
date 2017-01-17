import json
import uuid

import etcd

job_id1 = str(uuid.uuid4())

job = {
    "cluster_id": "49fa2adde8a6e98591f0f5cb4bc5f44d",
    "run": "tendrl.gluster_integration.flows.create_volume.CreateVolume",
    "status": 'new',
    "targeted_tags": [  # Refer flow definition file for details about this
        "tendrl/integration/gluster",
        "gluster/server"
    ],
    "parameters": {
        "Volume.volname": 'Volume1',
        "Volume.bricks": ["mntpath1", "mntpath2"]
    },
    type: "sds"
}


client = etcd.Client()
client.write("/queue/job_%s" % job_id1, json.dumps(job))
