from tendrl.commons.utils import cmd_utils
from tendrl.commons.utils import log_utils as logger


def get_brick_source_and_mount(brick_path):
    # source and target correspond to fields "Filesystem" and
    # "Mounted on" from df command output. The below command
    # gives the filesystem and mount point for a given path,
    # Eg: "/dev/mapper/tendrlMyBrick4_vg-tendrlMyBrick4_lv " \
    #     "/tendrl_gluster_bricks/MyBrick4_mount"

    command = "df --output=source,target " + brick_path.split(":")[-1]
    cmd = cmd_utils.Command(command)
    out, err, rc = cmd.run()
    if rc != 0:
        logger.log(
            "error",
            NS.publisher_id,
            {
                "message": "%s command failed: %s" % (
                    command, err
                )
            }
        )
        return None, None
    return out.split("\n")[-1].split()


def update_brick_device_details(brick_name, brick_path, devicetree, sync_ttl):
    mount_source, mount_point = get_brick_source_and_mount(brick_path)
    if not mount_source or not mount_point:
        logger.log(
            "error",
            NS.publisher_id,
            {
                "message": "Cound not update brick device details"
            }
        )
    if mount_point in devicetree.keys():
        brick = NS.tendrl.objects.GlusterBrick(
            NS.tendrl_context.integration_id,
            NS.node_context.fqdn,
            brick_dir=brick_name.split(":_")[-1]
        ).load()
        brick.devices = devicetree[mount_point].get("disks", [])
        brick.partitions = devicetree[mount_point].get("partitions", [])
        brick.pv = []
        if "device_info" in devicetree[mount_point]:
            brick.size = devicetree[mount_point]["device_info"].get("size", "")
            brick.mount_path = \
                devicetree[mount_point]["device_info"].get("mountpoint", "")
        brick.save(ttl=sync_ttl)
