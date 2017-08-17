import etcd


def sync_utilization_details(volumes):
    cluster_raw_capacity = 0
    cluster_used_capacity = 0
    cluster_usable_capacity = 0
    for volume in volumes:
        subvol_count = 0
        bricks = []
        raw_capacity = 0
        raw_used = 0
        up_bricks = 0
        subvols = []
        while True:
            try:
                subvol = NS._int.client.read(
                    "clusters/%s/Volumes/%s/Bricks/subvolume%s" % (
                        NS.tendrl_context.integration_id,
                        volume.vol_id,
                        subvol_count
                    )
                )
                subvol_bricks = []
                for entry in subvol.leaves:
                    brick_name = entry.key.split("/")[-1]
                    fetched_brick = NS.gluster.objects.Brick(
                        name=brick_name
                    ).load()
                    raw_capacity += fetched_brick.utilization['total']
                    cluster_raw_capacity += \
                        fetched_brick.utilization['total']
                    raw_used += fetched_brick.utilization['used']
                    cluster_used_capacity += \
                        fetched_brick.utilization['used']
                    bricks.append(fetched_brick)
                    subvol_bricks.append(fetched_brick)
                    if fetched_brick.status != "Started":
                        up_bricks += 1
                subvols.append(subvol_bricks)
                subvol_count += 1
            except etcd.EtcdKeyNotFound:
                break
        vol_usable_capacity = 0
        vol_used_capacity = 0
        vol_pcnt_used = 0
        if up_bricks == len(bricks):
            if int(volume.disperse_count) == 0:
                # This is distribute or replicate volume
                vol_usable_capacity = \
                    raw_capacity / int(volume.replica_count)
                vol_used_capacity = \
                    raw_used / int(volume.replica_count)
            else:
                # this is a disperse volume, with all bricks online
                # assumption : all bricks are the same size

                # Calculate the disperse yield as ratio of a difference
                # between disperse_count and redundancy count to actual
                # disperse_count. This is no %tage of actual data bricks
                # in the volume.
                disperse_yield = \
                    float(
                        int(volume.disperse_count) -
                        int(volume.redundancy_count)
                    ) / int(volume.disperse_count)
                vol_usable_capacity = raw_capacity * disperse_yield
                vol_used_capacity = raw_used * disperse_yield
        else:
            if int(volume.replica_count) > 1 or \
                int(volume.disperse_count) > 0:
                for subvol in subvols:
                    for brick in subvol:
                        if brick.status == "Started":
                            vol_usable_capacity += \
                                brick.utilization['total']
                            vol_used_capacity += \
                                brick.utilization['used']
                        # For replicate volume use only one replica
                        if int(volume.replica_count) > 1:
                            break
            else:
                vol_usable_capacity = raw_capacity
                vol_used_capacity = raw_used
        if vol_usable_capacity > 0:
            vol_pcnt_used = (
                vol_used_capacity / float(vol_usable_capacity)
            ) * 100
        volume.usable_capacity = vol_usable_capacity
        volume.used_capacity = vol_used_capacity
        volume.pcnt_used = str(vol_pcnt_used)
        volume.save()
        cluster_usable_capacity += volume.usable_capacity
    cluster_pcnt_used = 0
    if cluster_usable_capacity > 0:
        cluster_pcnt_used = (
            cluster_used_capacity / float(cluster_usable_capacity)
        ) * 100

    NS.gluster.objects.Utilization(
        raw_capacity=cluster_raw_capacity,
        used_capacity=cluster_used_capacity,
        usable_capacity=cluster_usable_capacity,
        pcnt_used=str(cluster_pcnt_used)
    ).save()
