import etcd

from tendrl.commons.utils import etcd_utils
from tendrl.commons.utils import event_utils
from tendrl.gluster_integration.objects.geo_replication_session\
    import GeoReplicationSessionStatus


RESOURCE_TYPE_VOLUME = "volume"


def save_georep_details(volumes, index):
    pair_index = 1
    while True:
        try:
            session_id = "{0}_{1}_{2}".format(
                volumes[
                    'volume%s.pair%s.master_volume' % (
                        index, pair_index
                    )
                ],
                volumes[
                    'volume%s.pair%s.slave' % (
                        index, pair_index)
                ].split("::")[-1],
                volumes[
                    'volume%s.pair%s.session_slave' % (
                        index, pair_index)
                ].split(":")[-1]
            )
            pair_name = "{0}-{1}".format(
                volumes[
                    'volume%s.pair%s.master_node' % (
                        index, pair_index)
                ],
                volumes[
                    'volume%s.pair%s.master_brick' % (
                        index, pair_index)
                ].replace("/", "_")
            )

            readable_pair_name = "{0}:{1}".format(
                volumes[
                    'volume%s.pair%s.master_node' % (
                        index, pair_index)
                ],
                volumes[
                    'volume%s.pair%s.master_brick' % (
                        index, pair_index)
                ]
            )

            try:
                pair = NS.gluster.objects.GeoReplicationPair(
                    vol_id=volumes['volume%s.id' % index],
                    session_id=session_id,
                    pair=pair_name
                ).load()
                fetched_pair_status = None
                if pair:
                    fetched_pair_status = pair.status
                pair_status = volumes[
                    'volume%s.pair%s.status' % (index, pair_index)
                ]
                if fetched_pair_status and \
                    fetched_pair_status != pair_status and \
                    pair_status.lower() == 'faulty':
                    msg = ("Geo-replication between %s "
                           "and %s is faulty") % (
                               readable_pair_name,
                               volumes['volume%s.name' % index])
                    instance = "volume_%s|georep_%s" % (
                        volumes['volume%s.name' % index],
                        pair_name
                    )
                    event_utils.emit_event(
                        "georep_status",
                        pair_status,
                        msg,
                        instance,
                        'WARNING',
                        tags={"entity_type": RESOURCE_TYPE_VOLUME,
                              "volume_name": volumes['volume%s.name' % index]
                              }
                    )
                if fetched_pair_status and \
                    fetched_pair_status.lower() == 'faulty' and \
                    pair_status.lower() in ['active', 'passive']:
                    msg = ("Geo-replication between %s "
                           "and %s is %s") % (
                               readable_pair_name,
                               volumes['volume%s.name' % index],
                               pair_status)
                    instance = "volume_%s|georep_%s" % (
                        volumes['volume%s.name' % index],
                        pair_name
                    )
                    event_utils.emit_event(
                        "georep_status",
                        pair_status,
                        msg,
                        instance,
                        'INFO',
                        tags={"entity_type": RESOURCE_TYPE_VOLUME,
                              "volume_name": volumes['volume%s.name' % index]
                              }
                    )
            except etcd.EtcdKeyNotFound:
                pass

            pair = NS.gluster.objects.GeoReplicationPair(
                vol_id=volumes['volume%s.id' % index],
                session_id=session_id,
                pair=pair_name,
                master_volume=volumes[
                    'volume%s.pair%s.master_volume' % (
                        index, pair_index)],
                master_brick=volumes[
                    'volume%s.pair%s.master_brick' % (
                        index, pair_index)],
                master_node=volumes[
                    'volume%s.pair%s.master_node' % (
                        index, pair_index)],
                slave_user=volumes[
                    'volume%s.pair%s.slave_user' % (
                        index, pair_index)],
                slave=volumes[
                    'volume%s.pair%s.slave' % (
                        index, pair_index)],
                slave_node=volumes[
                    'volume%s.pair%s.slave_node' % (
                        index, pair_index)],
                status=volumes[
                    'volume%s.pair%s.status' % (
                        index, pair_index)],
                crawl_status=volumes[
                    'volume%s.pair%s.crawl_status' % (
                        index, pair_index)],
                last_synced=volumes[
                    'volume%s.pair%s.last_synced' % (
                        index, pair_index)],
                entry=volumes[
                    'volume%s.pair%s.entry' % (
                        index, pair_index)],
                data=volumes[
                    'volume%s.pair%s.data' % (
                        index, pair_index)],
                meta=volumes[
                    'volume%s.pair%s.meta' % (
                        index, pair_index)],
                failures=volumes[
                    'volume%s.pair%s.failures' % (
                        index, pair_index)],
                checkpoint_time=volumes[
                    'volume%s.pair%s.checkpoint_time' % (
                        index, pair_index)],
                checkpoint_completed=volumes[
                    'volume%s.pair%s.checkpoint_completed' % (
                        index, pair_index)],
                checkpoint_completed_time=volumes[
                    'volume%s.pair%s.checkpoint_completion_time' % (
                        index, pair_index)]
            )
        except KeyError:
            break
        pair.save()
        pair_index += 1
    return


def aggregate_session_status():
    volumes = NS.tendrl.objects.GlusterVolume(
        NS.tendrl_context.integration_id
    ).load_all()
    georep_status = GeoReplicationSessionStatus()
    if volumes:
        for volume in volumes:
            vol_id = volume.vol_id
            sessions = None
            try:
                sessions = etcd_utils.read(
                    "clusters/%s/Volumes/%s/GeoRepSessions" % (
                        NS.tendrl_context.integration_id,
                        vol_id
                    )
                )
            except etcd.EtcdKeyNotFound:
                continue
            pair_count = int(volume.brick_count)
            for session in sessions.leaves:
                session_status = None
                session_id = session.key.split("GeoRepSessions/")[-1]
                pairs = NS.gluster.objects.GeoReplicationPair(
                    vol_id=vol_id,
                    session_id=session_id
                ).load_all()
                faulty_count = 0
                stopped_count = 0
                paused_count = 0
                created_count = 0
                for pair in pairs:
                    if pair.status.lower() == "faulty":
                        faulty_count += 1
                    elif pair.status.lower() == "created":
                        created_count += 1
                    elif pair.status.lower() == "stopped":
                        stopped_count += 1
                    elif pair.status.lower() == "paused":
                        paused_count += 1
                if created_count == pair_count:
                    session_status = georep_status.CREATED
                elif faulty_count == 0 and (
                        stopped_count == 0 and paused_count == 0 and (
                        created_count == 0
                        )
                ):
                    session_status = georep_status.UP
                elif pair_count == faulty_count:
                    session_status = georep_status.DOWN
                elif stopped_count == pair_count:
                    session_status = georep_status.STOPPED
                elif paused_count == pair_count:
                    session_status = georep_status.PAUSED
                else:
                    session_status = georep_status.PARTIAL
                NS.tendrl.objects.GeoReplicationSession(
                    vol_id=vol_id,
                    session_id=session_id,
                    session_status=session_status
                ).save()
