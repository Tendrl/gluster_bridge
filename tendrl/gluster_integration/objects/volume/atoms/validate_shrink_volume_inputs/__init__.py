import etcd
import json

from tendrl.commons.event import Event
from tendrl.commons.message import Message
from tendrl.commons import objects
from tendrl.gluster_integration.objects.volume import Volume


class ValidateShrinkVolumeInputs(objects.BaseAtom):
    def __init__(self, *args, **kwargs):
        super(ValidateShrinkVolumeInputs, self).__init__(*args, **kwargs)


    def _getBrickList(self, brick_count, sub_vol_len, volume_id):
        try:
            subvolumes = NS._int.client.read(
                "clusters/%s/Volumes/%s/Bricks" % (
                    NS.tendrl_context.integration_id,
                    volume_id
                ),
            )
        except etcd.EtcdKeyNotFound:
            Event(
                Message(
                    priority="error",
                    publisher=NS.publisher_id,
                    payload={
                        "message": "Volume %s does not have Bricks directory" %
                        self.parameters['Volume.volname']
                    },
                    job_id=self.parameters["job_id"],
                    flow_id=self.parameters["flow_id"],
                    cluster_id=NS.tendrl_context.integration_id,
                )
            )
            return []

        brick_list = []
        for subvolume in subvolumes.leaves:
            sub_vol = []
            bricks = NS._int.client.read(subvolume.key)
            for brick in bricks.leaves:
                sub_vol.append(brick.key.split("/")[-1])
            brick_list.append(sub_vol)

        return brick_list

    def _check_input_bricks(self, diff, input_bricks, brick_list):
        msg = ""
        for brick_set in input_bricks:
            if len(brick_set) != diff:
                msg = "Incorrect number of bricks provided for decreasing replica count" + \
                      "Each replica set needs %s. But %s given" % (
                          diff, len(brick_set)
                      )
                break
            lst = []
            for b in brick_set:
                brick_name = b.keys()[0] + ":" + b.values()[0].replace("/", "_")
                if not lst:
                    for el in brick_list:
                        if brick_name in el:
                            lst = el
                            break
                    else:
                        msg = "Brick provided not found in this volume"
                        break
                if brick_name in lst:
                    continue
                else:
                    msg = "Bricks provided in each sub list doesn't belong" +\
                          "to same replica set"
                    break
            brick_list.remove(lst)
        return msg

    def run(self):
        
        Event(
            Message(
                priority="info",
                publisher=NS.publisher_id,
                payload={
                    "message": "Checking if inputs for shrink vol are valid for %s" %
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
        replica_count = int(fetched_volume.replica_count)
        disperse_count = int(fetched_volume.disperse_count)
        brick_count = int(fetched_volume.brick_count)
        sub_vol_len = 0
        if replica_count > 1:
            sub_vol_len = replica_count
        elif disperse_count > 1:
            sub_vol_len = disperse_count
        brick_list = self._getBrickList(
            brick_count, sub_vol_len,
            self.parameters['Volume.vol_id']
        )
        if not brick_list:
            return False
        
        msg = ""
        # Check if its a replicated volume
        if replica_count > 1:
            # check if job is trying to change replica count
            if int(self.parameters.get("Volume.replica_count", replica_count))< replica_count:
                distribute_count = brick_count/replica_count
                diff = replica_count - int(self.parameters["Volume.replica_count"])
                # check if user is passing bricks for all distribute sets
                if len(self.parameters["Volume.bricks"]) != distribute_count:
                    msg = "Insufficient Bricks for reducing replica count" + \
                          "distribute-sets: %d, bricks-provided: %d" % (
                              distribute_count,
                              len(self.parameters["Volume.bricks"])
                          )
                else:
                    # check if all distribute sets have required number of bricks
                    msg = self._check_input_bricks(diff, self.parameters["Volume.bricks"], brick_list)
                        
            # check if user is trying to reduce the replica count using expand vol
            elif int(self.parameters.get("Volume.replica_count",replica_count))> replica_count:
                msg = "Can't increase the replica-count using shrink volume. %d > %d" % (
                    self.parameters["Volume.replica_count"], replica_count)
            # user is trying to remove a distribute set and we have to check if each
            # distribute set has requried number bricks(equal to replica_count)
            else:
                msg = self._check_input_bricks(replica_count, self.parameters["Volume.bricks"], brick_list)
        # user is trying to shrink dispersed volume, check if he is providing enough bricks(k+m)
        # for the sub-volume
        elif disperse_count > 1:
            msg = self._check_input_bricks(sub_vol_len, self.parameters["Volume.bricks"], brick_list)

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
                        "message": "Inputs for shrink volume validated successfully"
                    },
                    job_id=self.parameters["job_id"],
                    flow_id=self.parameters["flow_id"],
                    cluster_id=NS.tendrl_context.integration_id,
                )
            )
            return True
