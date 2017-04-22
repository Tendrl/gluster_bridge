def get_hosts(host_list):
    host = {
        "hosts": host_list
    }
    return host

def get_yum(action, packages, repos=None, gpgcheck="yes", update="no", target_host=""):
    yum = {
        "action": action,
        "packages": packages,
    }

    if action == "install":
        yum.update(
            {
                "gpgcheck": gpgcheck,
                "update": update
            }
        )
        if repos:
            yum.update(
                {"repos": repos}
            )
    section_header = "yum"
    if target_host:
        section_header += ":" + target_host
    return {section_header: yum}

def get_backend_setup(devices, vgs=None, pools=None, lvs=None,
                      lv_size=None, mount_points=None,
                      brick_dirs=None, target_host=""):
    backend_setup = {
        "devices": devices
    }
    if vgs:
        backend_setup.update(
            {"vgs": vgs}
        )
    if lvs:
        backend_setup.update(
            {"lvs": lvs}
        )
    if pools:
        backend_setup.update(
            {"pools": pools}
        )
    if lv_size:
        backend_setup.update(
            {"size": lv_size}
        )
    if mount_points:
        backend_setup.update(
            {"mountpoints": mount_points}
        )
    if brick_dirs:
        backend_setup.update(
            {"brick_dirs": brick_dirs}
        )
    section_header = "backend-setup"
    if target_host:
        section_header += ":" + target_host
    return {section_header: backend_setup}

def get_volume(volume_name, action, brick_dirs=None, transport=None,
               replica_count=None, disperse=None, disperse_count=None,
               redundancy_count=None, force="", target_host="",
               option_keys=[], option_values=[]):
    volume = {
        "volname": volume_name,
        "action": action
    }
    if brick_dirs:
        if action == "add-brick":
            volume.update({"bricks": brick_dirs})
        else:
            volume.update({"brick_dirs": brick_dirs})
    if transport:
        volume.update({"transport": transport})
    if replica_count:
        volume.update({"replica_count": replica_count})
    if disperse_count:
        volume.update({"disperse_count": disperse_count})
    if disperse:
        volume.update({"disperse": disperse})
    if redundancy_count:
        volume.update({"redundancy_count": redundancy_count})
    if force != "":
        volume.update({"force": "yes" if force else "no"})
    if option_keys:
        volume.update({"key": option_keys})
    if option_values:
        volume.update({"value": option_values})


    section_header = "volume"
    if target_host:
        section_header += ":" + target_host
    return {section_header: volume}

def get_snapshot(volume_name, action, snap_name, target_host=""):
    snapshot = {
        "action": action,
        "volname": volume_name,
        "snap_name": snap_name
    }
    
    section_header = "snapshot"
    if target_host:
        section_header += ":" + target_host
    return {section_header: snapshot}

def get_quota(volume_name, action, path=[], size=[], target_host=""):
    quota = {
        "volname": volume_name,
        "action": action
    }
    if path:
        quota.update(
            {
                "path": path,
                "size": size
            }
        )

    section_header = "quota"
    if target_host:
        section_header += ":" + target_host
    return {section_header: quota}
