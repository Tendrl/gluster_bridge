import etcd

from tendrl.commons.utils import cmd_utils


def sync_cluster_status(volumes):
    status = 'healthy'

    # Calculate status based on volumes status
    if len(volumes) > 0:
        volume_states = _derive_volume_states(volumes)
        for vol_id, state in volume_states.iteritems():
            if 'down' in state or 'partial' in state:
                status = 'unhealthy'

    # Change status basd on node status
    cmd = cmd_utils.Command(
        'gluster pool list', True
    )
    out, err, rc = cmd.run()
    if not err:
        out_lines = out.split('\n')
        connected = True
        for index in range(1, len(out_lines) - 1):
            node_status_det = out_lines[index].split('\t')
            if len(node_status_det) > 2:
                if node_status_det[2].strip() != 'Connected':
                    connected = connected and False
        if connected:
            status = 'healthy'
        else:
            status = 'unhealthy'

    # Persist the cluster status
    NS.gluster.objects.GlobalDetails(
        status=status
    ).save()


def _derive_volume_states(volumes):
    out_dict = {}
    for volume in volumes:
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
                        name=brick_name
                    ).load()
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
        if total_bricks == 0:
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
        # Save the volume status
        volume.state = out_dict[volume.vol_id]
        volume.save()
    return out_dict
