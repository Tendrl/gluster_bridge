import etcd

from tendrl.commons.event import Event
from tendrl.commons.message import Message
from tendrl.commons import objects
from tendrl.gluster_integration.objects.volume import Volume


class ValidateExpandVolumeInputs(objects.BaseAtom):
    def __init__(self, *args, **kwargs):
        super(ValidateExpandVolumeInputs, self).__init__(*args, **kwargs)

    def run(self):
        Event(
            Message(
                priority="info",
                publisher=NS.publisher_id,
                payload={
                    "message": "Checking if inputs for expand vol are valid for %s" %
                    self.parameters['Volume.volname']
                },
                job_id=self.parameters["job_id"],
                flow_id=self.parameters["flow_id"],
                cluster_id=NS.tendrl_context.integration_id,
            )
        )
        try:
            fetched_volume = Volume(
                vol_id=self.parameters['Volume.vol_id']
            ).load()
        except etcd.EtcdKeyNotFound:
            Event(
                Message(
                    priority="error",
                    publisher=NS.publisher_id,
                    payload={
                        "message": "Volume %s does not exist" %
                        self.parameters['Volume.volname']
                    },
                    job_id=self.parameters["job_id"],
                    flow_id=self.parameters["flow_id"],
                    cluster_id=NS.tendrl_context.integration_id,
                )
            )
            return False
        
        msg = ""
        replica_count = int(fetched_volume.replica_count)
        disperse_count = int(fetched_volume.disperse_count)
        # Check if its a replicated volume
        if replica_count > 1:
            # check if job is trying to change replica count
            if int(self.parameters.get("Volume.replica_count",replica_count)) > replica_count:
                distribute_count = int(fetched_volume.brick_count)/replica_count
                diff = int(self.parameters["Volume.replica_count"]) - replica_count
                # check if user is passing bricks for all distribute sets
                if len(self.parameters["Volume.bricks"]) != distribute_count:
                    msg = "Insufficient Bricks for increasing replica count" + \
                          "distribute-sets: %d, bricks-provided: %d" % (
                              distribute_count,
                              len(self.parameters["Volume.bricks"])
                          )
                else:
                    # check if all distribute sets have required number of bricks
                    for brick_set in self.parameters["Volume.bricks"]:
                        if len(brick_set) != diff:
                            msg = "Incorrect number of bricks provided for increasing replica count" + \
                                  "Each replica set needs %s. But %s given" % (
                                      diff, len(brick_set)
                                  )
                            break
            # check if user is trying to reduce the replica count using expand vol
            elif int(self.parameters.get("Volume.replica_count",replica_count)) < replica_count:
                msg = "Can't reduce the replica-count using expand volume. %d < %d" % (
                    self.parameters["Volume.replica_count"], replica_count)
            # user is trying to add new distribute set and we have to check if each
            # distribute set has requried number bricks(equal to replica_count)
            else:
                for brick_set in self.parameters["Volume.bricks"]:
                    if len(brick_set) != replica_count:
                        msg = "Incorrect number of bricks provided for Expand volume" + \
                                  "Each replica set needs %s bricks. But %s given" % (
                                      replica_count, len(brick_set)
                                  )
                        break
        # user is trying to expand dispersed volume, check if he is providing enough bricks(k+m)
        # for the new sub-volume
        elif disperse_count > 1:
            sub_vol_brick_count = disperse_count
            for brick_set in self.parameters["Volume.bricks"]:
                if len(brick_set) != sub_vol_brick_count:
                    msg = "Incorrect number of bricks provided for Expand volume" + \
                          "Each disperse sub-vol needs %s bricks. But %s given" % (
                              sub_vol_brick_count, len(brick_set)
                          )
                    break
            

        if msg:
            Event(
                Message(
                    priority="error",
                    publisher=NS.publisher_id,
                    payload={
                        "message": msg
                    },
                    job_id=self.parameters["job_id"],
                    flow_id=self.parameters["flow_id"],
                    cluster_id=NS.tendrl_context.integration_id,
                )
            )
            return False
        else:
            Event(
                Message(
                    priority="info",
                    publisher=NS.publisher_id,
                    payload={
                        "message": "Inputs for expand volume validated successfully"
                    },
                    job_id=self.parameters["job_id"],
                    flow_id=self.parameters["flow_id"],
                    cluster_id=NS.tendrl_context.integration_id,
                )
            )
            return True
