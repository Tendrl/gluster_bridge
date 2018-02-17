import blivet
import json
import re
import subprocess
import threading
import time

import etcd

from tendrl.commons.event import Event
from tendrl.commons.message import ExceptionMessage
from tendrl.commons.objects.cluster_alert_counters import \
    ClusterAlertCounters
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

        try:
            etcd_utils.read(
                "clusters/%s/"
                "cluster_network" % NS.tendrl_context.integration_id
            )
        except etcd.EtcdKeyNotFound:
            try:
                node_networks = etcd_utils.read(
                    "nodes/%s/Networks" % NS.node_context.node_id
                )
                # TODO(team) this logic needs to change later
                # multiple networks supported for gluster use case
                node_network = NS.tendrl.objects.NodeNetwork(
                    interface=node_networks.leaves.next().key.split('/')[-1]
                ).load()
                cluster = NS.tendrl.objects.Cluster(
                    integration_id=NS.tendrl_context.integration_id
                ).load()
                cluster.cluster_network = node_network.subnet
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
                if _cluster.status == "importing" and \
                    _cluster.current_job['status'] == 'failed':
                    continue

                _cluster.status = "syncing"
                _cluster.save()

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
                            peer = NS.gluster.\
                                objects.Peer(
                                    peer_uuid=peers['peer%s.uuid' % index],
                                    hostname=peers[
                                        'peer%s.primary_hostname' % index
                                    ],
                                    state=peers['peer%s.state' % index],
                                    connected=peers['peer%s.connected' % index]
                                )
                            try:
                                stored_peer_status = NS._int.client.read(
                                    "clusters/%s/Peers/%s/connected" % (
                                        NS.tendrl_context.integration_id,
                                        peers['peer%s.uuid' % index]
                                    )
                                ).value
                                current_status = peers[
                                    'peer%s.connected' % index
                                ]
                                if stored_peer_status != "" and \
                                    current_status != stored_peer_status:
                                    msg = (
                                        "Status of peer: %s in cluster %s "
                                        "changed from %s to %s"
                                    ) % (
                                        peers[
                                            'peer%s.primary_hostname' %
                                            index
                                        ],
                                        NS.tendrl_context.integration_id,
                                        stored_peer_status,
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
                                        'WARNING' if current_status !=
                                        'Connected'
                                        else 'INFO'
                                    )
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
                    index = 1
                    volumes = raw_data['Volumes']
                    while True:
                        try:
                            sync_volumes(
                                volumes, index,
                                raw_data_options.get('Volume Options'),
                                # sync_interval + 100 + no of peers + 350
                                SYNC_TTL + 350
                            )
                            index += 1
                            SYNC_TTL += 1
                        except KeyError:
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
                        NS.gluster.objects.VolumeOptions(
                            vol_id=vol_id,
                            options=dict1
                        ).save()

                # Sync cluster global details
                if "provisioner/%s" % NS.tendrl_context.integration_id \
                    in NS.node_context.tags:
                    all_volumes = NS.gluster.objects.Volume().load_all() or []
                    volumes = []
                    for volume in all_volumes:
                        if not str(volume.deleted).lower() == "true":
                            volumes.append(volume)
                    cluster_status.sync_cluster_status(volumes, SYNC_TTL + 350)
                    utilization.sync_utilization_details(volumes)
                    client_connections.sync_volume_connections(volumes)
                    georep_details.aggregate_session_status()
                    evt.process_events()
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

                _cluster = NS.tendrl.objects.Cluster(
                    integration_id=NS.tendrl_context.integration_id
                )
                if _cluster.exists():
                    _cluster = _cluster.load()
                    _cluster.status = ""
                    _cluster.last_sync = str(tendrl_now())
                    _cluster.is_managed = "yes"
                    _cluster.save()
                    # Initialize alert count
                    try:
                        alerts_count_key = '/clusters/%s/alert_counters' % (
                            NS.tendrl_context.integration_id)
                        etcd_utils.read(alerts_count_key)
                    except(etcd.EtcdException)as ex:
                        if type(ex) == etcd.EtcdKeyNotFound:
                            ClusterAlertCounters(
                                integration_id=NS.tendrl_context.integration_id
                            ).save()
                # check and enable volume profiling
                if "provisioner/%s" % NS.tendrl_context.integration_id in \
                    NS.node_context.tags:
                    self._enable_disable_volume_profiling()

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

    def _enable_disable_volume_profiling(self):
        cluster = NS.tendrl.objects.Cluster(
            integration_id=NS.tendrl_context.integration_id
        ).load()
        volumes = NS.gluster.objects.Volume().load_all() or []
        failed_vols = []
        for volume in volumes:
            if cluster.enable_volume_profiling == "yes":
                if volume.profiling_enabled == 'False' or \
                    volume.profiling_enabled == '':
                    action = "start"
                else:
                    continue
            else:
                if volume.profiling_enabled == 'True':
                    action = "stop"
                else:
                    continue
            out, err, rc = cmd_utils.Command(
                "gluster volume profile %s %s" %
                (volume.name, action)
            ).run()
            if err or rc != 0:
                if action == "start" and "already started" in err:
                    volume.profiling_enabled = "True"
                if action == "stop" and "not started" in err:
                    volume.profiling_enabled = "False"
                volume.save()
                failed_vols.append(volume.name)
                continue
            else:
                volume.profiling_enabled = \
                    "True" if cluster.enable_volume_profiling == \
                    "yes" else "False"
                volume.save()
        if len(failed_vols) > 0:
            logger.log(
                "warning",
                NS.publisher_id,
                {"message": "%sing profiling failed for volumes: %s" %
                 (action, str(failed_vols))}
            )


def sync_volumes(volumes, index, vol_options, sync_ttl):
    # instantiating blivet class, this will be used for
    # getting brick_device_details
    b = blivet.Blivet()

    # reset blivet during every sync to get latest information
    # about storage devices in the machine
    b.reset()
    devicetree = b.devicetree
    node_context = NS.node_context.load()
    tag_list = node_context.tags
    # Raise alerts for volume state change.
    cluster_provisioner = "provisioner/%s" % NS.tendrl_context.integration_id
    if cluster_provisioner in tag_list:
        try:
            stored_volume_status = NS._int.client.read(
                "clusters/%s/Volumes/%s/status" % (
                    NS.tendrl_context.integration_id,
                    volumes['volume%s.id' % index]
                )
            ).value
            current_status = volumes['volume%s.status' % index]
            if stored_volume_status != "" and \
                current_status != stored_volume_status:
                msg = ("Status of volume: %s in cluster %s "
                       "changed from %s to %s") % (
                           volumes['volume%s.name' % index],
                           NS.tendrl_context.integration_id,
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

        volume = NS.gluster.objects.Volume(
            vol_id=volumes['volume%s.id' % index],
            vol_type="arbiter"
            if int(volumes['volume%s.arbiter_count' % index]) > 0
            else volumes['volume%s.type' % index],
            name=volumes['volume%s.name' % index],
            transport_type=volumes['volume%s.transport_type' % index],
            status=volumes['volume%s.status' % index],
            brick_count=volumes['volume%s.brickcount' % index],
            snap_count=volumes['volume%s.snap_count' % index],
            stripe_count=volumes['volume%s.stripe_count' % index],
            replica_count=volumes['volume%s.replica_count' % index],
            subvol_count=volumes['volume%s.subvol_count' % index],
            arbiter_count=volumes['volume%s.arbiter_count' % index],
            disperse_count=volumes['volume%s.disperse_count' % index],
            redundancy_count=volumes['volume%s.redundancy_count' % index],
            quorum_status=volumes['volume%s.quorum_status' % index],
            snapd_status=volumes['volume%s.snapd_svc.online_status' % index],
            snapd_inited=volumes['volume%s.snapd_svc.inited' % index],
        )
        volume.save(ttl=sync_ttl)

        # Initialize volume alert count
        try:
            volume_alert_count_key = '/clusters/%s/Volumes/%s/'\
                                     'alert_counters' % (
                                         NS.tendrl_context.integration_id,
                                         volumes['volume%s.id' % index]
                                     )
            etcd_utils.read(volume_alert_count_key)
        except(etcd.EtcdException)as ex:
            if type(ex) == etcd.EtcdKeyNotFound:
                NS.gluster.objects.VolumeAlertCounters(
                    integration_id=NS.tendrl_context.integration_id,
                    volume_id=volumes['volume%s.id' % index]
                ).save()
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
        NS.gluster.objects.VolumeOptions(
            vol_id=volume.vol_id,
            options=vol_opt_dict
        ).save(ttl=sync_ttl)

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
        networks = NS._int.client.read(
            "nodes/%s/Networks" % NS.node_context.
            node_id
        )
        for interface in networks.leaves:
            key = interface.key.split("/")[-1]
            network = NS.tendrl.objects.NodeNetwork(
                interface=key
            ).load()
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
            if (NS.node_context.fqdn != hostname) and (
                hostname not in network_ip):
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
                sbs = NS._int.client.read(
                    "clusters/%s/Bricks/all/"
                    "%s/%s/status" % (
                        NS.tendrl_context.
                        integration_id,
                        NS.node_context.fqdn,
                        brick_name.split(":_")[-1]
                    )
                ).value
                current_status = volumes.get(
                    'volume%s.brick%s.status' % (index, b_index)
                )
                if current_status != sbs:
                    msg = ("Status of brick: %s "
                           "under volume %s in cluster %s chan"
                           "ged from %s to %s") % (
                               volumes['volume%s.brick%s' '.path' % (
                                   index,
                                   b_index
                               )],
                               volumes['volume%s.' 'name' % index],
                               NS.tendrl_context.integration_id,
                               sbs,
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

            NS._int.wclient.write(vol_brick_path, "")

            brick = NS.gluster.objects.Brick(
                NS.node_context.fqdn,
                brick_name.split(":_")[-1],
                name=brick_name,
                vol_id=volumes['volume%s.id' % index],
                sequence_number=b_index,
                brick_path=volumes[
                    'volume%s.brick%s.path' % (index, b_index)
                ],
                hostname=volumes.get(
                    'volume%s.brick%s.hostname' % (index, b_index)
                ),
                port=volumes.get(
                    'volume%s.brick%s.port' % (index, b_index)
                ),
                vol_name=volumes['volume%s.name' % index],
                used=True,
                node_id=NS.node_context.node_id,
                status=volumes.get(
                    'volume%s.brick%s.status' % (index, b_index)
                ),
                filesystem_type=volumes.get(
                    'volume%s.brick%s.filesystem_type' % (index, b_index)
                ),
                mount_opts=volumes.get(
                    'volume%s.brick%s.mount_options' % (index, b_index)
                ),
                utilization=brick_utilization.brick_utilization(
                    volumes['volume%s.brick%s.path' % (index, b_index)]
                ),
                client_count=volumes.get(
                    'volume%s.brick%s.client_count' % (index, b_index)
                ),
                is_arbiter=volumes.get(
                    'volume%s.brick%s.is_arbiter' % (index, b_index)
                ),
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
            bricks = NS.gluster.objects.Brick(
                fqdn=hostname
            ).load_all()
            for brick in bricks:
                if brick.status.lower() == BRICK_STARTED:
                    # raise an alert for brick
                    msg = (
                        "Status of brick: %s "
                        "under volume %s in cluster %s chan"
                        "ged from %s to %s") % (
                            brick.brick_path,
                            brick.vol_name,
                            NS.tendrl_context.integration_id,
                            BRICK_STARTED.title(),
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
