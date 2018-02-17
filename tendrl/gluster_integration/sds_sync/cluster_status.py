import etcd

from tendrl.commons.utils import cmd_utils
from tendrl.commons.utils import event_utils


RESOURCE_TYPE_VOLUME = "volume"


def sync_cluster_status(volumes, sync_ttl):
    # Calculate status based on volumes status
    degraded_count = 0
    is_healthy = True
    if len(volumes) > 0:
        volume_states = _derive_volume_states(volumes)
        for vol_id, state in volume_states.iteritems():
            if 'down' in state or 'partial' in state:
                is_healthy = False
            if 'degraded' in state:
                degraded_count += 1

    # Change status basd on node status
    cmd = cmd_utils.Command(
        'gluster pool list', True
    )
    out, err, rc = cmd.run()
    peer_count = 0
    if not err:
        out_lines = out.split('\n')
        connected = True
        for index in range(1, len(out_lines)):
            peer_count += 1
            node_status_det = out_lines[index].split('\t')
            if len(node_status_det) > 2:
                if node_status_det[2].strip() != 'Connected':
                    connected = connected and False
        if not connected:
            is_healthy = False

    cluster_gd = NS.gluster.objects.GlobalDetails().load()
    old_status = cluster_gd.status or 'unhealthy'
    curr_status = 'healthy' if is_healthy else 'unhealthy'
    if curr_status != old_status:
        msg = ("Health status of cluster: %s "
               "changed from %s to %s") % (
                   NS.tendrl_context.integration_id,
                   old_status,
                   curr_status)
        instance = "cluster_%s" % NS.tendrl_context.integration_id
        event_utils.emit_event(
            "cluster_health_status",
            curr_status,
            msg,
            instance,
            'WARNING' if curr_status == 'unhealthy'
            else 'INFO'
        )

    # Persist the cluster status
    NS.gluster.objects.GlobalDetails(
        status='healthy' if is_healthy else 'unhealthy',
        peer_count=peer_count,
        vol_count=len(volumes),
        volume_up_degraded=degraded_count
    ).save(ttl=sync_ttl)


def _derive_volume_states(volumes):
    out_dict = {}
    for volume in volumes:
        if volume.status == "Stopped":
            out_dict[volume.vol_id] = "down"
        else:
            subvol_count = 0
            bricks = []
            subvol_states = []
            while True:
                try:
                    subvol = NS._int.client.read(
                        "clusters/%s/Volumes/%s/Bricks/subvolume%s" % (
                            NS.tendrl_context.integration_id,
                            volume.vol_id,
                            subvol_count
                        )
                    )
                    state = 0
                    for entry in subvol.leaves:
                        brick_name = entry.key.split("/")[-1]
                        fetched_brick = NS.gluster.objects.Brick(
                            brick_name.split(":")[0],
                            brick_name.split(":_")[-1]
                        ).load()
                        if not fetched_brick.status:
                            fetched_brick.status = "Stopped"
                        bricks.append(fetched_brick)
                        if fetched_brick.status != "Started":
                            state += 1
                    subvol_states.append(state)
                    subvol_count += 1
                except etcd.EtcdKeyNotFound:
                    break

            total_bricks = len(bricks)
            up_bricks = 0
            for brick in bricks:
                if brick.status == "Started":
                    up_bricks += 1
            if total_bricks == 0 or total_bricks < int(volume.brick_count):
                # No brick details updated for the volume yet
                out_dict[volume.vol_id] = 'unknown'
            elif up_bricks == 0:
                out_dict[volume.vol_id] = 'down'
            else:
                out_dict[volume.vol_id] = 'up'
                if int(volume.replica_count) > 1 or \
                    int(volume.disperse_count) > 0:
                    worst_subvol = max(subvol_states)
                    if worst_subvol > 0:
                        subvol_prob = max(
                            int(volume.replica_count),
                            int(volume.redundancy_count) + 1
                        )
                        if worst_subvol == subvol_prob:
                            # if this volume contains only one subvolume,
                            # and the bricks down > redundancy level
                            # then the volume state needs to show down
                            if subvol_count == 1:
                                out_dict[volume.vol_id] = 'down'
                            else:
                                out_dict[volume.vol_id] = '(partial)'
                        else:
                            out_dict[volume.vol_id] = '(degraded)'
                else:
                    # This volume is not 'protected', so any brick
                    # disruption leads straight to a 'partial'
                    # availability state
                    if up_bricks != total_bricks:
                        out_dict[volume.vol_id] = '(partial)'
        # Raise the alert if volume state changes
        if volume.state != "" and \
            out_dict[volume.vol_id] != volume.state:
            msg = "State of volume: %s " \
                  "changed from %s to %s" % (
                      volume.name,
                      volume.state,
                      out_dict[volume.vol_id]
                  )
            instance = "volume_%s" % volume.name
            event_utils.emit_event(
                "volume_state",
                out_dict[volume.vol_id],
                msg,
                instance,
                'INFO' if out_dict[volume.vol_id] == 'up' else 'WARNING',
                tags={"entity_type": RESOURCE_TYPE_VOLUME,
                      "volume_name": volume.name
                      }
            )
        # Save the volume status
        volume.state = out_dict[volume.vol_id]
        volume.save()

    return out_dict
