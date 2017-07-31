from tendrl.commons.event import Event
from tendrl.commons.message import Message
from tendrl.commons.utils import cmd_utils
import xml.etree.cElementTree as etree


def get_rebalance_status(volume):
    cmd = cmd_utils.Command('gluster volume status %s --xml' % volume)
    out, err, rc = cmd.run()
    if rc != 0:
        Event(
            Message(
                priority="error",
                publisher=NS.publisher_id,
                payload={"message": "volume status command failed: %s" % err}
            )
        )

    tree = etree.fromstring(out)
    rv = int(tree.find('opRet').text)
    msg = tree.find('opErrstr').text
    if rv != 0:
        Event(
            Message(
                priority="error",
                publisher=NS.publisher_id,
                payload={"message": "volume status command failed: %s" % msg}
            )
        )

    for task in tree.findall('volStatus/volumes/volume/tasks'):
        if task and task.find('task/type').text == "Rebalance":
            return task.find('task/statusStr').text
    return None


def sync_volume_rebalance_estimated_time(volumes):
    for volume in volumes:
        rebal_estimated_time = 0
        vol_rebal_details = NS.gluster.objects.RebalanceDetails(
            vol_id=volume.vol_id
        ).load_all()
        for entry in vol_rebal_details:
            if entry.time_left and \
                int(entry.time_left) > rebal_estimated_time:
                rebal_estimated_time = int(entry.time_left)
        volume.rebal_estimated_time = rebal_estimated_time
        volume.save()
