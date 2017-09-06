import blivet
import etcd
import gevent
import gevent.hub
import json
import re
import subprocess

from tendrl.commons.event import Event
from tendrl.commons.message import ExceptionMessage
from tendrl.commons.message import Message
from tendrl.commons import sds_sync
from tendrl.commons.utils import cmd_utils
from tendrl.commons.utils import etcd_utils
from tendrl.commons.utils import log_utils as logger
from tendrl.commons.utils import monitoring_utils
from tendrl.commons.utils.time_utils import now as tendrl_now
from tendrl.gluster_integration import ini2json
from tendrl.gluster_integration.sds_sync import brick_device_details
from tendrl.gluster_integration.sds_sync import brick_utilization
from tendrl.gluster_integration.sds_sync import client_connections
from tendrl.gluster_integration.sds_sync import cluster_status
from tendrl.gluster_integration.sds_sync import event_utils
from tendrl.gluster_integration.sds_sync import georep_details
from tendrl.gluster_integration.sds_sync import rebalance_status
from tendrl.gluster_integration.sds_sync import snapshots
from tendrl.gluster_integration.sds_sync import utilization

gevent.hub.Hub.NOT_ERROR = (Exception,)

RESOURCE_TYPE_BRICK = "brick"
RESOURCE_TYPE_PEER = "host"
RESOURCE_TYPE_VOLUME = "volume"


class GlusterIntegrationSdsSyncStateThread(sds_sync.SdsSyncThread):

    def __init__(self):
        super(GlusterIntegrationSdsSyncStateThread, self).__init__()
        self._complete = gevent.event.Event()

    def _run(self):
        # To detect out of band deletes
        # refresh gluster object inventory at config['sync_interval']
        # Default is 260 seconds
        SYNC_TTL = int(NS.config.data.get("sync_interval", 10)) + 250
        Event(
            Message(
                priority="info",
                publisher=NS.publisher_id,
                payload={"message": "%s running" % self.__class__.__name__}
            )
        )

        gluster_brick_dir = NS.gluster.objects.GlusterBrickDir()
        gluster_brick_dir.save()

        cluster = NS.tendrl.objects.Cluster(
            integration_id=NS.tendrl_context.integration_id
        ).load()
        if cluster.cluster_network is None or\
            cluster.cluster_network == "":
            try:
                node_networks = etcd_utils.read(
                    "nodes/%s/Networks" % NS.node_context.node_id
                )
                # TODO(team) this logic needs to change later
                # multiple networks supported for gluster use case
                node_network = NS.tendrl.objects.NodeNetwork(
                    interface=node_networks.leaves.next().key.split('/')[-1]
                ).load()
                cluster.cluster_network = node_network.subnet
                cluster.save()
            except etcd.EtcdKeyNotFound as ex:
                Event(
                    Message(
                        priority="error",
                        publisher=NS.publisher_id,
                        payload={
                            "message": "Failed to sync cluster network details"
                        }
                    )
                )
                raise ex

        NS.node_context = NS.tendrl.objects.NodeContext().load()
        while not self._complete.is_set():
            try:
                gevent.sleep(
                    int(NS.config.data.get("sync_interval", 10))
                )
                try:
                    cluster = NS.tendrl.objects.Cluster(
                        integration_id=NS.tendrl_context.integration_id
                    ).load()
                    cluster.sync_status = "in_progress"
                    cluster.save()
                except (etcd.EtcdAlreadyExist, etcd.EtcdCompareFailed) as ex:
                    pass

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
                    while True:
                        try:
                            peer = NS.gluster.\
                                objects.Peer(
                                    peer_uuid=peers['peer%s.uuid' % index],
                                    hostname=peers[
                                        'peer%s.primary_hostname' % index
                                    ],
                                    state=peers['peer%s.state' % index]
                                )
                            if not peer.exists():
                                job_id = monitoring_utils.update_dashboard(
                                    peer.hostname,
                                    RESOURCE_TYPE_PEER,
                                    NS.tendrl_context.integration_id,
                                    "add"
                                )
                                logger.log(
                                    "debug",
                                    NS.publisher_id,
                                    {
                                        "message": "Update dashboard job %s "
                                        "created" % job_id
                                    }
                                )
                            peer.save(ttl=SYNC_TTL)
                            index += 1
                        except KeyError:
                            break
                if "Volumes" in raw_data:
                    index = 1
                    volumes = raw_data['Volumes']
                    while True:
                        g = gevent.spawn(
                            sync_volumes,
                            volumes,
                            index,
                            raw_data_options.get('Volume Options')
                        )
                        g.join()
                        if not g.successful() and \
                            g.exception.__class__.__name__ == 'KeyError':
                            break
                        index += 1
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
                        volume = NS.gluster.objects.Volume(
                            vol_id=vol_id
                        ).load()
                        dest = dict(volume.options)
                        dest.update(dict1)
                        volume.options = dest
                        volume.save()

                # Sync cluster global details
                if "provisioner/%s" % NS.tendrl_context.integration_id \
                    in NS.node_context.tags:
                    volumes = NS.gluster.objects.Volume().load_all() or []
                    cluster_status.sync_cluster_status(volumes)
                    utilization.sync_utilization_details(volumes)
                    client_connections.sync_volume_connections(volumes)
                    georep_details.aggregate_session_status()
                    rebalance_status.sync_volume_rebalance_status(volumes)
                    rebalance_status.sync_volume_rebalance_estimated_time(
                        volumes
                    )
                    snapshots.sync_volume_snapshots(
                        raw_data['Volumes'],
                        SYNC_TTL
                    )

                _cluster = NS.tendrl.objects.Cluster(
                    integration_id=NS.tendrl_context.integration_id
                ).load()
                if _cluster.exists():
                    _cluster.sync_status = "done"
                    _cluster.last_sync = str(tendrl_now())
                    _cluster.is_managed = "yes"
                    _cluster.save()

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
                raise ex

        Event(
            Message(
                priority="debug",
                publisher=NS.publisher_id,
                payload={"message": "%s complete" % self.__class__.__name__}
            )
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
                failed_vols.append(volume.name)
                continue
            volume.profiling_enabled = "True" if cluster.\
                                       enable_volume_profiling ==\
                                       "yes" else "False"
            volume.save()
        if len(failed_vols) > 0:
            Event(
                Message(
                    priority="warning",
                    publisher=NS.publisher_id,
                    payload={
                        "message": "%sing profiling failed for volumes: %s" %
                        (action, str(failed_vols))
                    }
                )
            )


def sync_volumes(volumes, index, vol_options):
    # instantiating blivet class, this will be used for
    # getting brick_device_details
    b = blivet.Blivet()

    # reset blivet during every sync to get latest information
    # about storage devices in the machine
    b.reset()
    devicetree = b.devicetree

    SYNC_TTL = int(NS.config.data.get("sync_interval", 10)) + 250
    NS.node_context = NS.tendrl.objects.NodeContext().load()
    tag_list = NS.node_context.tags
    # Raise alerts for volume state change.
    cluster_provisioner = "provisioner/%s" % NS.tendrl_context.integration_id
    if cluster_provisioner in tag_list:
        try:
            stored_volume = NS.gluster.objects.Volume(
                vol_id=volumes['volume%s.id' % index]
            ).load()
            current_status = volumes['volume%s.status' % index]
            if stored_volume.status and stored_volume.status != "" \
                and current_status != stored_volume.status:
                msg = "Status of volume: %s " + \
                      "changed from %s to %s" % (
                          volumes['volume%s.name' % index],
                          stored_volume.status,
                          current_status
                      )
                instance = "volume_%s" % volumes[
                    'volume%s.name' % index
                ]
                event_utils.emit_event(
                    "volume_status",
                    current_status,
                    msg,
                    instance
                )
        except etcd.EtcdKeyNotFound:
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
        if not volume.exists():
            job_id = monitoring_utils.update_dashboard(
                volume.name,
                RESOURCE_TYPE_VOLUME,
                NS.tendrl_context.integration_id,
                "add"
            )
            logger.log(
                "debug",
                NS.publisher_id,
                {
                    "message": "Update dashboard job %s "
                    "created" % job_id
                }
            )
        volume.save(ttl=SYNC_TTL)

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
    rebal_det.save(ttl=SYNC_TTL)
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
                stored_brick = NS.gluster.objects.Brick(
                    name=brick_name
                ).load()
                current_status = volumes.get(
                    'volume%s.brick%s.status' % (index, b_index)
                )
                if stored_brick.status and \
                    current_status != stored_brick.status:
                    msg = "Status of brick: %s " + \
                          "under volume %s chan" + \
                          "ged from %s to %s" % (
                              volumes['volume%s.brick%s' '.path' % (
                                  index,
                                  b_index
                              )],
                              volumes['volume%s.' 'name' % index],
                              stored_brick.status,
                              current_status
                          )
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
                        instance
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
                brick_name,
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
            if not brick.exists():
                job_id = monitoring_utils.update_dashboard(
                    brick.brick_name,
                    RESOURCE_TYPE_BRICK,
                    NS.tendrl_context.integration_id,
                    "add"
                )
                logger.log(
                    "debug",
                    NS.publisher_id,
                    {
                        "message": "Update dashboard job %s created" % job_id
                    }
                )
            brick.save(ttl=SYNC_TTL)
            # sync brick device details
            brick_device_details.\
                update_brick_device_details(
                    brick_name,
                    volumes[
                        'volume%s.brick%s.path' % (
                            index, b_index)
                    ],
                    devicetree
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
                        ).save(ttl=SYNC_TTL)
                    except KeyError:
                        break
                    c_index += 1
            b_index += 1
        except KeyError:
            break
