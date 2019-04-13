import json
import re
import socket
import subprocess
import threading
import time

import etcd

from tendrl.commons.event import Event
from tendrl.commons.message import ExceptionMessage
from tendrl.commons import sds_sync
from tendrl.commons.utils import cmd_utils
from tendrl.commons.utils import etcd_utils
from tendrl.commons.utils import event_utils
from tendrl.commons.utils import log_utils as logger
from tendrl.commons.utils.time_utils import now as tendrl_now
from tendrl.gluster_integration import ini2json
from tendrl.gluster_integration.message import process_events as evt
from tendrl.gluster_integration.sds_sync import brick_device_details
from tendrl.gluster_integration.sds_sync import brick_utilization
from tendrl.gluster_integration.sds_sync import client_connections
from tendrl.gluster_integration.sds_sync import cluster_status
from tendrl.gluster_integration.sds_sync import georep_details
from tendrl.gluster_integration.sds_sync import rebalance_status
from tendrl.gluster_integration.sds_sync import snapshots
from tendrl.gluster_integration.sds_sync import utilization


RESOURCE_TYPE_BRICK = "brick"
RESOURCE_TYPE_PEER = "host"
RESOURCE_TYPE_VOLUME = "volume"
BRICK_STOPPED = "stopped"
BRICK_STARTED = "started"


VOLUME_TTL = 350


class GlusterIntegrationSdsSyncStateThread(sds_sync.SdsSyncThread):

    def __init__(self):
        super(GlusterIntegrationSdsSyncStateThread, self).__init__()
        self._complete = threading.Event()

    def run(self):
        logger.log(
            "info",
            NS.publisher_id,
            {"message": "%s running" % self.__class__.__name__}
        )

        gluster_brick_dir = NS.gluster.objects.GlusterBrickDir()
        gluster_brick_dir.save()

        cluster = NS.tendrl.objects.Cluster(
            integration_id=NS.tendrl_context.integration_id
        ).load()
        if cluster.cluster_network in [None, ""]:
            try:
                node_networks = NS.tendrl.objects.NodeNetwork().load_all()
                cluster.cluster_network = node_networks[0].subnet
                cluster.save()
            except etcd.EtcdKeyNotFound as ex:
                logger.log(
                    "error",
                    NS.publisher_id,
                    {"message": "Failed to sync cluster network details"}
                )
        _sleep = 0
        while not self._complete.is_set():
            # To detect out of band deletes
            # refresh gluster object inventory at config['sync_interval']
            SYNC_TTL = int(NS.config.data.get("sync_interval", 10)) + 100
            NS.node_context = NS.node_context.load()
            NS.tendrl_context = NS.tendrl_context.load()
            if _sleep > 5:
                _sleep = int(NS.config.data.get("sync_interval", 10))
            else:
                _sleep += 1

            try:
                _cluster = NS.tendrl.objects.Cluster(
                    integration_id=NS.tendrl_context.integration_id
                ).load()
                if (_cluster.status == "importing" and (
                    _cluster.current_job['status'] == 'failed')) or \
                    _cluster.status == "unmanaging" or \
                    _cluster.status == "set_volume_profiling" or \
                    (_cluster.is_managed == "no" and
                     _cluster.current_job.get('status') != "in_progress"):
                    time.sleep(_sleep)
                    continue

                _cnc = NS.tendrl.objects.ClusterNodeContext(
                    node_id=NS.node_context.node_id
                ).load()
                _cnc.is_managed = "yes"
                _cnc.save()
                subprocess.call(
                    [
                        'gluster',
                        'get-state',
                        'glusterd',
                        'odir',
                        '/var/run',
                        'file',
                        'glusterd-state',
                        'detail'
                    ]
                )
                raw_data = ini2json.ini_to_dict(
                    '/var/run/glusterd-state'
                )
                subprocess.call(['rm', '-rf', '/var/run/glusterd-state'])
                subprocess.call(
                    [
                        'gluster',
                        'get-state',
                        'glusterd',
                        'odir',
                        '/var/run',
                        'file',
                        'glusterd-state-vol-opts',
                        'volumeoptions'
                    ]
                )
                raw_data_options = ini2json.ini_to_dict(
                    '/var/run/glusterd-state-vol-opts'
                )
                subprocess.call(
                    [
                        'rm',
                        '-rf',
                        '/var/run/glusterd-state-vol-opts'
                    ]
                )
                sync_object = NS.gluster.objects.\
                    SyncObject(data=json.dumps(raw_data))
                sync_object.save()

                if "Peers" in raw_data:
                    index = 1
                    peers = raw_data["Peers"]
                    disconnected_hosts = []
                    while True:
                        try:
                            peer = NS.tendrl.\
                                objects.GlusterPeer(
                                    peer_uuid=peers['peer%s.uuid' % index],
                                    hostname=peers[
                                        'peer%s.primary_hostname' % index
                                    ],
                                    state=peers['peer%s.state' % index],
                                    connected=peers['peer%s.connected' % index]
                                )
                            try:
                                stored_peer_status = None
                                # find peer detail using hostname
                                ip = socket.gethostbyname(
                                    peers['peer%s.primary_hostname' % index]
                                )
                                node_id = etcd_utils.read(
                                    "/indexes/ip/%s" % ip
                                ).value
                                stored_peer = NS.tendrl.objects.GlusterPeer(
                                    peer_uuid=peers['peer%s.uuid' % index],
                                    node_id=node_id
                                ).load()
                                stored_peer_status = stored_peer.connected
                                current_status = peers[
                                    'peer%s.connected' % index
                                ]
                                if stored_peer_status and \
                                    current_status != stored_peer_status:
                                    msg = (
                                        "Peer %s in cluster %s "
                                        "is %s"
                                    ) % (
                                        peers[
                                            'peer%s.primary_hostname' %
                                            index
                                        ],
                                        _cluster.short_name,
                                        current_status
                                    )
                                    instance = "peer_%s" % peers[
                                        'peer%s.primary_hostname' % index
                                    ]
                                    event_utils.emit_event(
                                        "peer_status",
                                        current_status,
                                        msg,
                                        instance,
                                        'WARNING'
                                        if current_status != 'Connected'
                                        else 'INFO'
                                    )
                                    # save current status in actual peer
                                    # directory also
                                    stored_peer.connected = current_status
                                    stored_peer.save()
                                    # Disconnected host name to
                                    # raise brick alert
                                    if current_status.lower() == \
                                        "disconnected":
                                        disconnected_hosts.append(
                                            peers[
                                                'peer%s.primary_hostname' %
                                                index
                                            ]
                                        )
                            except etcd.EtcdKeyNotFound:
                                pass
                            SYNC_TTL += 5
                            peer.save(ttl=SYNC_TTL)
                            index += 1
                        except KeyError:
                            break
                    # Raise an alert for bricks when peer disconnected
                    # or node goes down
                    for disconnected_host in disconnected_hosts:
                        brick_status_alert(
                            disconnected_host
                        )
                if "Volumes" in raw_data:
                    # create devicetree using lsblk
                    devicetree = get_device_tree()
                    # find lvs
                    lvs = brick_utilization.get_lvs()
                    index = 1
                    volumes = raw_data['Volumes']
                    total_brick_count = 0
                    while True:
                        try:
                            b_count = sync_volumes(
                                volumes, index,
                                raw_data_options.get('Volume Options'),
                                SYNC_TTL + VOLUME_TTL,
                                _cluster.short_name,
                                devicetree,
                                lvs
                            )
                            index += 1
                            SYNC_TTL += 1
                            total_brick_count += b_count - 1
                        except KeyError:
                            global VOLUME_TTL
                            # from second sync volume ttl is
                            # SYNC_TTL + (no.volumes) * 20 +
                            # (no.of.bricks) * 10 + 160
                            if index > 1:
                                volume_count = index - 1
                                # When all nodes are down we are updating all
                                # volumes are down, node status TTL is 160,
                                # So make sure volumes are present in etcd
                                # while raising volume down alert
                                VOLUME_TTL = (volume_count * 20) + (
                                    total_brick_count * 10) + 160
                            break
                    # populate the volume specific options
                    reg_ex = re.compile("^volume[0-9]+.options+")
                    options = {}
                    for key in volumes.keys():
                        if reg_ex.match(key):
                            options[key] = volumes[key]
                    for key in options.keys():
                        volname = key.split('.')[0]
                        vol_id = volumes['%s.id' % volname]
                        dict1 = {}
                        for k, v in options.items():
                            if k.startswith('%s.options' % volname):
                                dict1['.'.join(k.split(".")[2:])] = v
                                options.pop(k, None)
                        volume = NS.tendrl.objects.GlusterVolume(
                            NS.tendrl_context.integration_id,
                            vol_id=vol_id
                        ).load()
                        if volume.options is not None:
                            dest = dict(volume.options)
                            dest.update(dict1)
                            volume.options = dest
                            volume.save()

                # Sync cluster global details
                if "provisioner/%s" % NS.tendrl_context.integration_id \
                    in NS.node_context.tags:
                    all_volumes = NS.tendrl.objects.GlusterVolume(
                        NS.tendrl_context.integration_id
                    ).load_all() or []
                    volumes = []
                    for volume in all_volumes:
                        if not str(volume.deleted).lower() == "true" and \
                            volume.current_job.get('status', '') \
                            in ['', 'finished', 'failed'] and \
                            volume.vol_id not in [None, ''] and \
                            volume.name not in [None, '']:
                            # only for first sync refresh volume TTL
                            # It will increase TTL based on no.of volumes
                            if _cnc.first_sync_done in [None, "no", ""]:
                                etcd_utils.refresh(
                                    volume.value,
                                    SYNC_TTL + VOLUME_TTL
                                )
                            volumes.append(volume)
                    cluster_status.sync_cluster_status(
                        volumes, SYNC_TTL + VOLUME_TTL
                    )
                    utilization.sync_utilization_details(volumes)
                    client_connections.sync_volume_connections(volumes)
                    georep_details.aggregate_session_status()
                    try:
                        evt.process_events()
                    except etcd.EtcdKeyNotFound:
                        pass
                    rebalance_status.sync_volume_rebalance_status(volumes)
                    rebalance_status.sync_volume_rebalance_estimated_time(
                        volumes
                    )
                    snapshots.sync_volume_snapshots(
                        raw_data['Volumes'],
                        int(NS.config.data.get(
                            "sync_interval", 10
                        )) + len(volumes) * 4
                    )
                    # update alert count
                    update_cluster_alert_count()
                # check and enable volume profiling
                if "provisioner/%s" % NS.tendrl_context.integration_id in \
                    NS.node_context.tags:
                    self._update_volume_profiling()

                _cluster = NS.tendrl.objects.Cluster(
                    integration_id=NS.tendrl_context.integration_id
                ).load()
                if _cluster.exists():
                    _cluster = _cluster.load()
                    _cluster.last_sync = str(tendrl_now())
                    # Mark the first sync done flag
                    _cnc = NS.tendrl.objects.ClusterNodeContext(
                        node_id=NS.node_context.node_id
                    ).load()
                    if _cnc.first_sync_done in [None, "no"]:
                        _cnc.first_sync_done = "yes"
                        _cnc.save()
                    if _cluster.current_job.get(
                        'status', ''
                    ) in ['', 'finished', 'failed'] and \
                        _cluster.status in [None, ""]:
                        _cluster.save()
            except Exception as ex:
                Event(
                    ExceptionMessage(
                        priority="error",
                        publisher=NS.publisher_id,
                        payload={"message": "gluster sds state sync error",
                                 "exception": ex
                                 }
                    )
                )
            try:
                etcd_utils.read(
                    '/clusters/%s/_sync_now' %
                    NS.tendrl_context.integration_id
                )
                continue
            except etcd.EtcdKeyNotFound:
                pass

            time.sleep(_sleep)

        logger.log(
            "debug",
            NS.publisher_id,
            {"message": "%s complete" % self.__class__.__name__}
        )

    def _update_volume_profiling(self):
        cluster = NS.tendrl.objects.Cluster(
            integration_id=NS.tendrl_context.integration_id
        ).load()
        volumes = NS.tendrl.objects.GlusterVolume(
            NS.tendrl_context.integration_id
        ).load_all() or []
        profiling_enabled_count = 0
        profiling_unknown_count = 0
        for volume in volumes:
            if volume.profiling_enabled == "yes":
                profiling_enabled_count += 1
            if volume.profiling_enabled in [None, ""]:
                profiling_unknown_count += 1
        if profiling_unknown_count == len(volumes):
            return
        if profiling_enabled_count == 0:
            cluster.volume_profiling_state = "disabled"
        elif profiling_enabled_count == len(volumes):
            cluster.volume_profiling_state = "enabled"
        elif profiling_enabled_count < len(volumes):
            cluster.volume_profiling_state = "mixed"
        volume_profiling_state = cluster.volume_profiling_state
        cluster = cluster.load()
        cluster.volume_profiling_state = volume_profiling_state
        cluster.save()


def sync_volumes(
    volumes, index,
    vol_options,
    sync_ttl,
    cluster_short_name,
    devicetree,
    lvs
):
    NS.node_context = NS.tendrl.objects.NodeContext().load()
    tag_list = NS.node_context.tags
    # Raise alerts for volume state change.
    cluster_provisioner = "provisioner/%s" % NS.tendrl_context.integration_id
    if cluster_provisioner in tag_list:
        try:
            _volume = NS.tendrl.objects.GlusterVolume(
                NS.tendrl_context.integration_id,
                vol_id=volumes['volume%s.id' % index]
            ).load()
            if _volume.locked_by and 'job_id' in _volume.locked_by and \
                _volume.current_job.get('status', '') == 'in_progress':
                # There is a job active on volume. skip the sync
                return
            stored_volume_status = _volume.status
            current_status = volumes['volume%s.status' % index]
            if stored_volume_status not in [None, ""] and \
                current_status != stored_volume_status:
                msg = ("Status of volume: %s in cluster %s "
                       "changed from %s to %s") % (
                           volumes['volume%s.name' % index],
                           cluster_short_name,
                           stored_volume_status,
                           current_status)
                instance = "volume_%s" % volumes[
                    'volume%s.name' % index
                ]
                event_utils.emit_event(
                    "volume_status",
                    current_status,
                    msg,
                    instance,
                    'WARNING' if current_status == 'Stopped'
                    else 'INFO',
                    tags={"entity_type": RESOURCE_TYPE_VOLUME,
                          "volume_name": volumes['volume%s.name' % index]
                          }
                )
        except (KeyError, etcd.EtcdKeyNotFound) as ex:
            if isinstance(ex, KeyError):
                raise ex
            pass

        volume = NS.tendrl.objects.GlusterVolume(
            NS.tendrl_context.integration_id,
            vol_id=volumes['volume%s.id' % index]
        ).load()
        volume.vol_type = "arbiter" \
            if int(volumes['volume%s.arbiter_count' % index]) > 0 \
            else volumes['volume%s.type' % index]
        volume.name = volumes['volume%s.name' % index]
        volume.transport_type = volumes['volume%s.transport_type' % index]
        volume.status = volumes['volume%s.status' % index]
        volume.brick_count = volumes['volume%s.brickcount' % index]
        volume.snap_count = volumes['volume%s.snap_count' % index]
        volume.stripe_count = volumes['volume%s.stripe_count' % index]
        volume.replica_count = volumes['volume%s.replica_count' % index]
        volume.subvol_count = volumes['volume%s.subvol_count' % index]
        volume.arbiter_count = volumes['volume%s.arbiter_count' % index]
        volume.disperse_count = volumes['volume%s.disperse_count' % index]
        volume.redundancy_count = volumes['volume%s.redundancy_count' % index]
        volume.quorum_status = volumes['volume%s.quorum_status' % index]
        volume.snapd_status = volumes[
            'volume%s.snapd_svc.online_status' % index]
        volume.snapd_inited = volumes['volume%s.snapd_svc.inited' % index]
        if NS.tendrl.objects.GlusterVolume(
            NS.tendrl_context.integration_id,
            vol_id=volumes['volume%s.id' % index]
        ).exists():
            existing_vol = NS.tendrl.objects.GlusterVolume(
                NS.tendrl_context.integration_id,
                vol_id=volumes['volume%s.id' % index]
            ).load()
            volume_profiling_old_value = existing_vol.profiling_enabled
        else:
            volume_profiling_old_value = volume.profiling_enabled
        if ('volume%s.profile_enabled' % index) in volumes:
            value = int(volumes['volume%s.profile_enabled' % index])
            if value == 1:
                volume_profiling_new_value = "yes"
            else:
                volume_profiling_new_value = "no"
        else:
            volume_profiling_new_value = None
        volume.profiling_enabled = volume_profiling_new_value
        if volume_profiling_old_value not in [None, ""] and \
            volume_profiling_old_value != volume_profiling_new_value:
            # Raise alert for the same value change
            msg = ("Value of volume profiling for volume: %s "
                   "of cluster %s changed from %s to %s" % (
                       volumes['volume%s.name' % index],
                       cluster_short_name,
                       volume_profiling_old_value,
                       volume_profiling_new_value))
            instance = "volume_%s" % \
                volumes['volume%s.name' % index]
            event_utils.emit_event(
                "volume_profiling_status",
                volume_profiling_new_value,
                msg,
                instance,
                'INFO',
                tags={
                    "entity_type": RESOURCE_TYPE_BRICK,
                    "volume_name": volumes[
                        'volume%s.name' % index
                    ]
                }
            )
        volume.save(ttl=sync_ttl)
        # Save the default values of volume options
        vol_opt_dict = {}
        for opt_count in \
            range(1, int(vol_options['volume%s.options.count' % index])):
            vol_opt_dict[
                vol_options[
                    'volume%s.options.key%s' % (index, opt_count)
                ]
            ] = vol_options[
                'volume%s.options.value%s' % (index, opt_count)
            ]
        volume.options = vol_opt_dict
        volume.save()

    rebal_det = NS.gluster.objects.RebalanceDetails(
        vol_id=volumes['volume%s.id' % index],
        rebal_id=volumes['volume%s.rebalance.id' % index],
        rebal_status=volumes['volume%s.rebalance.status' % index],
        rebal_failures=volumes['volume%s.rebalance.failures' % index],
        rebal_skipped=volumes['volume%s.rebalance.skipped' % index],
        rebal_lookedup=volumes['volume%s.rebalance.lookedup' % index],
        rebal_files=volumes['volume%s.rebalance.files' % index],
        rebal_data=volumes['volume%s.rebalance.data' % index],
        time_left=volumes.get('volume%s.rebalance.time_left' % index),
    )
    rebal_det.save(ttl=sync_ttl)
    georep_details.save_georep_details(volumes, index)

    b_index = 1
    # ipv4 address of current node
    try:
        network_ip = []
        networks = NS.tendrl.objects.NodeNetwork().load_all()
        for network in networks:
            if network.ipv4:
                network_ip.extend(network.ipv4)
    except etcd.EtcdKeyNotFound as ex:
        Event(
            ExceptionMessage(
                priority="debug",
                publisher=NS.publisher_id,
                payload={
                    "message": "Could not find "
                    "any ipv4 networks for node"
                    " %s" % NS.node_context.node_id,
                    "exception": ex
                }
            )
        )
    while True:
        try:
            # Update brick node wise
            hostname = volumes[
                'volume%s.brick%s.hostname' % (index, b_index)
            ]
            ip = socket.gethostbyname(hostname)
            try:
                node_id = etcd_utils.read("indexes/ip/%s" % ip).value
                fqdn = NS.tendrl.objects.ClusterNodeContext(
                    node_id=node_id
                ).load().fqdn
                cluster_node_ids = etcd_utils.read(
                    "indexes/tags/tendrl/integration/%s" %
                    NS.tendrl_context.integration_id
                ).value
                cluster_node_ids = json.loads(cluster_node_ids)
                if NS.node_context.fqdn != fqdn or \
                        node_id not in cluster_node_ids:
                    b_index += 1
                    continue
            except(TypeError, etcd.EtcdKeyNotFound):
                b_index += 1
                continue
            sub_vol_size = (int(
                volumes['volume%s.brickcount' % index]
            )) / int(
                volumes['volume%s.subvol_count' % index]
            )
            brick_name = NS.node_context.fqdn
            brick_name += ":"
            brick_name += volumes['volume%s.brick%s' '.path' % (
                index,
                b_index
            )].split(":")[-1].replace("/", "_")

            # Raise alerts if the brick path changes
            try:
                stored_brick = NS.tendrl.objects.GlusterBrick(
                    NS.tendrl_context.integration_id,
                    NS.node_context.fqdn,
                    brick_dir=brick_name.split(":_")[-1]
                ).load()
                current_status = volumes.get(
                    'volume%s.brick%s.status' % (index, b_index)
                )
                if stored_brick.status and \
                    current_status != stored_brick.status:
                    msg = ("Brick:%s in volume:%s has %s"
                           ) % (
                               volumes['volume%s.brick%s' '.path' % (
                                   index,
                                   b_index
                               )],
                               volumes['volume%s.' 'name' % index],
                               current_status)
                    instance = "volume_%s|brick_%s" % (
                        volumes['volume%s.name' % index],
                        volumes['volume%s.brick%s.path' % (
                            index,
                            b_index
                        )]
                    )
                    event_utils.emit_event(
                        "brick_status",
                        current_status,
                        msg,
                        instance,
                        'WARNING' if current_status == 'Stopped'
                        else 'INFO',
                        tags={"entity_type": RESOURCE_TYPE_BRICK,
                              "volume_name": volumes[
                                  'volume%s.' 'name' % index]
                              }
                    )

            except etcd.EtcdKeyNotFound:
                pass

            brk_pth = "clusters/%s/Volumes/%s/Bricks/subvolume%s/%s"

            vol_brick_path = brk_pth % (
                NS.tendrl_context.integration_id,
                volumes['volume%s.id' % index],
                str((b_index - 1) / sub_vol_size),
                brick_name
            )

            etcd_utils.write(vol_brick_path, "")
            brick = NS.tendrl.objects.GlusterBrick(
                NS.tendrl_context.integration_id,
                NS.node_context.fqdn,
                brick_dir=brick_name.split(":_")[-1]
            ).load()
            brick.integration_id = NS.tendrl_context.integration_id
            brick.fqdn = NS.node_context.fqdn
            brick.brick_dir = brick_name.split(":_")[-1]
            brick.name = brick_name
            brick.vol_id = volumes['volume%s.id' % index]
            brick.sequence_number = b_index
            brick.brick_path = volumes[
                'volume%s.brick%s.path' % (index, b_index)
            ]
            brick.hostname = volumes.get(
                'volume%s.brick%s.hostname' % (index, b_index)
            )
            brick.port = volumes.get(
                'volume%s.brick%s.port' % (index, b_index)
            )
            brick.vol_name = volumes['volume%s.name' % index]
            brick.used = True
            brick.node_id = NS.node_context.node_id
            brick.status = volumes.get(
                'volume%s.brick%s.status' % (index, b_index)
            )
            brick.filesystem_type = volumes.get(
                'volume%s.brick%s.filesystem_type' % (index, b_index)
            )
            brick.mount_opts = volumes.get(
                'volume%s.brick%s.mount_options' % (index, b_index)
            )
            brick.utilization = brick_utilization.brick_utilization(
                volumes['volume%s.brick%s.path' % (index, b_index)],
                lvs
            )
            brick.client_count = volumes.get(
                'volume%s.brick%s.client_count' % (index, b_index)
            )
            brick.is_arbiter = volumes.get(
                'volume%s.brick%s.is_arbiter' % (index, b_index)
            )
            brick.save(ttl=sync_ttl)
            # sync brick device details
            brick_device_details.\
                update_brick_device_details(
                    brick_name,
                    volumes[
                        'volume%s.brick%s.path' % (
                            index, b_index)
                    ],
                    devicetree,
                    sync_ttl
                )

            # Sync the brick client details
            c_index = 1
            if volumes.get(
                'volume%s.brick%s.client_count' % (index, b_index)
            ) > 0:
                while True:
                    try:
                        NS.gluster.objects.ClientConnection(
                            brick_name=brick_name,
                            fqdn=NS.node_context.fqdn,
                            brick_dir=brick_name.split(":_")[-1],
                            hostname=volumes[
                                'volume%s.brick%s.client%s.hostname' % (
                                    index, b_index, c_index
                                )
                            ],
                            bytesread=volumes[
                                'volume%s.brick%s.client%s.bytesread' % (
                                    index, b_index, c_index
                                )
                            ],
                            byteswrite=volumes[
                                'volume%s.brick%s.client%s.byteswrite' % (
                                    index, b_index, c_index
                                )
                            ],
                            opversion=volumes[
                                'volume%s.brick%s.client%s.opversion' % (
                                    index, b_index, c_index
                                )
                            ]
                        ).save(ttl=sync_ttl)
                    except KeyError:
                        break
                    c_index += 1
            sync_ttl += 4
            b_index += 1
        except KeyError:
            break
    return b_index


def brick_status_alert(hostname):
    try:
        # fetching brick details of disconnected node
        lock = None
        path = "clusters/%s/Bricks/all/%s" % (
            NS.tendrl_context.integration_id,
            hostname
        )
        lock = etcd.Lock(
            NS._int.client,
            path
        )
        lock.acquire(
            blocking=True,
            lock_ttl=60
        )
        if lock.is_acquired:
            bricks = NS.tendrl.objects.GlusterBrick(
                NS.tendrl_context.integration_id,
                fqdn=hostname
            ).load_all()
            for brick in bricks:
                if brick.status.lower() == BRICK_STARTED:
                    # raise an alert for brick
                    msg = (
                        "Brick:%s in volume:%s has %s") % (
                            brick.brick_path,
                            brick.vol_name,
                            BRICK_STOPPED.title()
                        )
                    instance = "volume_%s|brick_%s" % (
                        brick.vol_name,
                        brick.brick_path,
                    )
                    event_utils.emit_event(
                        "brick_status",
                        BRICK_STOPPED.title(),
                        msg,
                        instance,
                        'WARNING',
                        tags={"entity_type": RESOURCE_TYPE_BRICK,
                              "volume_name": brick.vol_name,
                              "node_id": brick.node_id,
                              "fqdn": brick.hostname
                              }
                    )
                    # Update brick status as stopped
                    brick.status = BRICK_STOPPED.title()
                    brick.save()
                    lock.release()
    except (
        etcd.EtcdException,
        KeyError,
        ValueError,
        AttributeError
    ) as ex:
        Event(
            ExceptionMessage(
                priority="error",
                publisher=NS.publisher_id,
                payload={
                    "message": "Unable to raise an brick status "
                               "alert for host %s" % hostname,
                    "exception": ex
                }
            )
        )
    finally:
        if isinstance(lock, etcd.lock.Lock) and lock.is_acquired:
            lock.release()


def update_cluster_alert_count():
    cluster_alert_count = 0
    severity = ["WARNING", "CRITICAL"]
    try:
        alert_counts = get_volume_alert_counts()
        alerts = NS.tendrl.objects.ClusterAlert(
            tags={'integration_id': NS.tendrl_context.integration_id}
        ).load_all()
        for alert in alerts:
            alert.tags = json.loads(alert.tags)
            if alert.severity in severity:
                cluster_alert_count += 1
                if alert.resource in NS.gluster.objects.VolumeAlertCounters(
                        )._defs['relationship'][alert.alert_type.lower()]:
                    vol_name = alert.tags.get('volume_name', None)
                    if vol_name and vol_name in alert_counts.keys():
                        alert_counts[vol_name]['alert_count'] += 1
        # Update cluster alert count
        NS.tendrl.objects.ClusterAlertCounters(
            integration_id=NS.tendrl_context.integration_id,
            alert_count=cluster_alert_count
        ).save()
        # Update volume alert count
        for volume, vol_dict in alert_counts.iteritems():
            NS.gluster.objects.VolumeAlertCounters(
                integration_id=NS.tendrl_context.integration_id,
                alert_count=vol_dict['alert_count'],
                volume_id=vol_dict['vol_id']
            ).save()
    except (etcd.EtcdException, AttributeError) as ex:
        logger.log(
            "debug",
            NS.publisher_id,
            {"message": "Unable to update alert count.err: %s" % ex}
        )


def get_volume_alert_counts():
    alert_counts = {}
    volumes = NS.tendrl.objects.GlusterVolume(
        NS.tendrl_context.integration_id
    ).load_all()
    for volume in volumes:
        if volume and volume.vol_id not in [None, ''] and \
                volume.name not in [None, '']:
            alert_counts[volume.name] = {'vol_id': volume.vol_id,
                                         'alert_count': 0
                                         }
    return alert_counts


def get_device_tree():
    """{

            "/boot" : {
                "disks": ['/dev/sda'],
                "partitions": ['/dev/sda1'],
                "device_info": {
                    "type": "part",
                    "mountpoint": "/boot",
                    "size": 44023414784
                },
            },
            "/gluster/brick1": {
                "disks": ['/dev/sdb'],
                "partitions": ['/dev/sdb1'],
                "device_info": {
                    "type": "lvm",
                    "mountpoint": "/gluster/brick1",
                    "size": 41873833984
                },
            },
            "/gluster/brick2": {
                "disks": ['/dev/sdc', '/dev/sde'],
                "partitions": ['/dev/sdc1', '/dev/sde1`'],
                "device_info": {
                    "type": "lvm",
                    "mountpoint": "/gluster/brick2",
                    "size": 40231763968
                },
            }

    }
    """

    devicetree = {}
    try:
        columns = 'NAME,KNAME,PKNAME,TYPE,MOUNTPOINT,SIZE'
        keys = columns.split(',')
        lsblk = ("lsblk --all --bytes --noheadings "
                 "--output='%s' --path --raw" % columns)
        cmd = cmd_utils.Command(lsblk)
        out, err, rc = cmd.run()
        if not err:
            out = out.encode('utf8')
            devlist = map(
                lambda line: dict(zip(keys, line.split(' '))),
                out.splitlines()
            )
            block_devices = {}
            for dev_info in devlist:
                if dev_info["TYPE"] == "disk":
                    # create new block device list
                    block_devices = {dev_info["KNAME"]: dev_info}
                else:
                    block_devices[dev_info["KNAME"]] = dev_info
                # find ancestors if mountpoint present, current block device
                # also considered as its ancestor
                if dev_info["MOUNTPOINT"]:
                    ancestor_details = find_ancestors(
                        block_devices, dev_info["KNAME"]
                    )
                    if dev_info["MOUNTPOINT"] in devicetree.keys():
                        # if different block device mounted in same mountpoint
                        # then append the disk and partition information under
                        # same mountpoint
                        disks = devicetree[dev_info["MOUNTPOINT"]]["disks"]
                        devicetree[dev_info["MOUNTPOINT"]]["disks"] = list(
                            set(disks + ancestor_details["disks"])
                        )
                        devicetree[dev_info["MOUNTPOINT"]]["partitions"] = \
                            list(
                                set(ancestor_details["partitions"])
                            )
                    else:
                        devicetree[dev_info["MOUNTPOINT"]] = {}
                        devicetree[dev_info["MOUNTPOINT"]]["disks"] = \
                            ancestor_details["disks"]
                        devicetree[dev_info["MOUNTPOINT"]]["partitions"] = \
                            ancestor_details["partitions"]
                        devicetree[dev_info["MOUNTPOINT"]]["device_info"] = {}
                        devicetree[dev_info["MOUNTPOINT"]]["device_info"][
                            "size"] = dev_info["SIZE"]
                        devicetree[dev_info["MOUNTPOINT"]]["device_info"][
                            "mountpoint"] = dev_info["MOUNTPOINT"]
                        devicetree[dev_info["MOUNTPOINT"]]["device_info"][
                            "type"] = dev_info["TYPE"]
    except (KeyError, TypeError, ValueError, AttributeError) as ex:
        logger.log(
            "error",
            NS.publisher_id,
            {"message": "Unable to generate devicetree.err: %s" % ex}
        )
    return devicetree


def find_ancestors(block_devices, device_name):
    # find ancestors bottom to top of its device hierarchy
    # If mount point found for a block device then find
    # ancestors for that device using its parent's,
    # if the device is not a disk then loop through using its parent name,
    # if parent also not disk then loop through its parent's parent name,
    # this process will continue until parent with disk type present
    ancestor_details = {}
    ancestor_details["disks"] = []
    ancestor_details["partitions"] = []
    while True:
        if device_name in block_devices.keys():
            if block_devices[device_name]["TYPE"] == "disk":
                ancestor_details["disks"].append(
                    block_devices[device_name]["NAME"]
                )
                break
            elif block_devices[device_name]["TYPE"] == "part":
                ancestor_details["partitions"].append(
                    block_devices[device_name]["NAME"]
                )
                device_name = block_devices[device_name]["PKNAME"]
            else:
                # find next ancestors from next parent_name
                device_name = block_devices[device_name]["PKNAME"]
        else:
            break
    return ancestor_details
