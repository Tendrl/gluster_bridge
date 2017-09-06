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


def update_brick_device_details(brick_name, brick_path, devicetree):
    mount_source, mount_point = get_brick_source_and_mount(brick_path)
    if not mount_source or not mount_point:
        logger.log(
            "error",
            NS.publisher_id,
            {
                "message": "Cound not update brick device details"
            }
        )
        return

    device = devicetree.resolveDevice(mount_source)
    size = int(device.size.to_integral())
    lv = None
    pool = None
    vg = None
    pvs = None
    disks = [d.path for d in device.ancestors if d.isDisk and not d.parents]

    if device.type in ("lvmthinlv", "lvmlv"):
        lv = device.name
        if hasattr(device, "pool"):
            pool = device.pool.name
        vg = device.vg.name
        pvs = [dev.path for dev in device.disks]

    brick = NS.gluster.objects.Brick(
        name=brick_name
    ).load()
    brick.devices = disks
    brick.mount_path = mount_point
    brick.lv = lv
    brick.vg = vg
    brick.pool = pool
    brick.pv = pvs
    brick.size = size
    brick.save()
