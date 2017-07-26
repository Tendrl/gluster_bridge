import etcd
import gevent
import json
import os
import re
import socket
import subprocess

from tendrl.commons.event import Event
from tendrl.commons.message import ExceptionMessage
from tendrl.commons.message import Message
from tendrl.commons import sds_sync
from tendrl.commons.utils import cmd_utils
from tendrl.commons.utils import etcd_utils
from tendrl.commons.utils.time_utils import now as tendrl_now
from tendrl.gluster_integration import ini2json
from tendrl.gluster_integration.sds_sync import brick_utilization
from tendrl.gluster_integration.sds_sync import rebalance_status as rebal_stat


class GlusterIntegrationSdsSyncStateThread(sds_sync.SdsSyncThread):

    def __init__(self):
        super(GlusterIntegrationSdsSyncStateThread, self).__init__()
        self._complete = gevent.event.Event()

    def _emit_event(self, resource, curr_value, msg, instance):
        alert = {}
        alert['source'] = NS.publisher_id
        alert['pid'] = os.getpid()
        alert['time_stamp'] = tendrl_now().isoformat()
        alert['alert_type'] = 'status'
        severity = "INFO"
        if curr_value.lower() == "stopped":
            severity = "CRITICAL"
        alert['severity'] = severity
        alert['resource'] = resource
        alert['current_value'] = curr_value
        alert['tags'] = dict(
            plugin_instance=instance,
            message=msg,
            cluster_id=NS.tendrl_context.integration_id,
            cluster_name=NS.tendrl_context.cluster_name,
            sds_name=NS.tendrl_context.sds_name,
            fqdn=socket.getfqdn()
        )
        alert['node_id'] = NS.node_context.node_id
        if not NS.node_context.node_id:
            return
        Event(
            Message(
                "notice",
                "alerting",
                {'message': json.dumps(alert)}
            )
        )

    def _sync_cluster_utilization_details(self, volumes):
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
                        cluster_raw_capacity += fetched_brick.utilization['total']
                        raw_used += fetched_brick.utilization['used']
                        cluster_used_capacity += fetched_brick.utilization['used']
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
                    vol_usable_capacity = raw_capacity / int(volume.replica_count)
                    vol_used_capacity = raw_used / int(volume.replica_count)
                else:
                    # this is a disperse volume, with all bricks online
                    # assumption : all bricks are the same size
                    disperse_yield = \
                        float(
                            int(volume.disperse_count) - int(volume.redundancy_count)
                        ) / int(volume.disperse_count)
                    vol_usable_capacity = raw_capacity * disperse_yield
                    vol_used_capacity = raw_used * disperse_yield
            else:
                if int(volume.replica_count) > 1 or int(volume.disperse_count) > 0:
                    for subvol in subvols:
                        for brick in subvol:
                            if brick.status == "Started":
                                vol_usable_capacity += brick.utilization['total']
                                vol_used_capacity += brick.utilization['used']
                            # For replicate volume use only one replica
                            if int(volume.replica_count) > 1:
                                break
                else:
                    vol_usable_capacity = raw_capacity
                    vol_used_capacity = raw_used
            if vol_usable_capacity > 0:
                vol_pcnt_used = (vol_used_capacity / float(vol_usable_capacity)) * 100
            volume.usable_capacity = vol_usable_capacity
            volume.used_capacity = vol_used_capacity
            volume.pcnt_used = str(vol_pcnt_used)
            volume.save()
            cluster_usable_capacity += volume.usable_capacity
        cluster_pcnt_used = 0
        if cluster_usable_capacity > 0:
            cluster_pcnt_used = (cluster_used_capacity / float(cluster_usable_capacity)) * 100

        NS.gluster.objects.Utilization(
            raw_capacity=cluster_raw_capacity,
            used_capacity=cluster_used_capacity,
            usable_capacity=cluster_usable_capacity,
            pcnt_used=str(cluster_pcnt_used)
        ).save()

    def _sync_cluster_status(self, volumes):
        status = 'healthy'

        # Calculate status based on volumes status
        if len(volumes) > 0:
            volume_states = self._derive_volume_states(volumes)
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

    def _derive_volume_states(self, volumes):
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
                if int(volume.replica_count) > 1 or int(volume.disperse_count) > 0:
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

    def _sync_volume_connections(self, volumes):
        for volume in volumes:
            subvol_count = 0
            vol_connections = 0
            while True:
                try:
                    subvol = NS._int.client.read(
                        "clusters/%s/Volumes/%s/Bricks/subvolume%s" % (
                            NS.tendrl_context.integration_id,
                            volume.vol_id,
                            subvol_count
                        )
                    )
                    for entry in subvol.leaves:
                        brick_name = entry.key.split("/")[-1]
                        fetched_brick = NS.gluster.objects.Brick(
                            name=brick_name
                        ).load()
                        vol_connections += int(fetched_brick.client_count)
                    subvol_count += 1
                except etcd.EtcdKeyNotFound:
                    break
            volume.client_count = vol_connections
            volume.save()

    def _sync_volume_rebalance_estimated_time(self, volumes):
        for volume in volumes:
            rebal_estimated_time = 0
            vol_rebal_details = NS.gluster.objects.RebalanceDetails(
                vol_id=volume.vol_id
            ).load_all()
            for entry in vol_rebal_details:
                if entry.time_left and \
                    int(entry.time_left) > rebal_estimated_time:
                    rebal_estimated_time = int(entry.time_left)
            volume.rebal_estimated_time = rebal_estimated_time
            volume.save()

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

        while not self._complete.is_set():
            try:
                gevent.sleep(
                    int(NS.config.data.get("sync_interval", 10))
                )
                try:
                    NS._int.wclient.write(
                        "clusters/%s/"
                        "sync_status" % NS.tendrl_context.integration_id,
                        "in_progress",
                        prevExist=False
                    )
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
                raw_data = ini2json.ini_to_dict('/var/run/glusterd-state')
                subprocess.call(['rm', '-rf', '/var/run/glusterd-state'])
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
                            peer.save(ttl=SYNC_TTL)
                            index += 1
                        except KeyError:
                            break
                if "Volumes" in raw_data:
                    index = 1
                    volumes = raw_data['Volumes']
                    while True:
                        g = gevent.spawn(sync_volumes, volumes, index)
                        g.join()
                        if not g.successful() and \
                            g.exception.__class__.__name__ == 'KeyError':
                            break
                        index += 1
                    # populate the volume options
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
                        vol_options = NS.gluster.objects.\
                            VolumeOptions(
                                vol_id=vol_id,
                                options=dict1
                            )
                        vol_options.save()

                # Sync cluster global details
                volumes = NS.gluster.objects.Volume().load_all()
                self._sync_cluster_status(volumes)
                self._sync_cluster_utilization_details(volumes)
                self._sync_volume_connections(volumes)
                self._sync_volume_rebalance_estimated_time(volumes)

                _cluster = NS.tendrl.objects.Cluster(
                    integration_id=NS.tendrl_context.integration_id
                )
                if _cluster.exists():
                    _cluster.sync_status = "done"
                    _cluster.last_sync = str(tendrl_now())
                    _cluster.is_managed = "yes"
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
                raise ex

        Event(
            Message(
                priority="debug",
                publisher=NS.publisher_id,
                payload={"message": "%s complete" % self.__class__.__name__}
            )
        )

def sync_volumes(volumes, index):
    SYNC_TTL = int(NS.config.data.get("sync_interval", 10)) + 250
    node_context = NS.node_context.load()
    tag_list = list(node_context.tags)
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
                msg = "Status of volume: %s " + \
                      "changed from %s to %s" % (
                          volumes['volume%s.name' % index],
                          stored_volume_status,
                          current_status
                      )
                instance = "volume_%s" % volumes[
                    'volume%s.name' % index
                ]
                self._emit_event(
                    "volume_status",
                    current_status,
                    msg,
                    instance
                )
        except etcd.EtcdKeyNotFound:
            pass

    rebalance_status = ""
    if volumes['volume%s.type' % index].startswith("Distribute"):
        status = rebal_stat.get_rebalance_status(
            volumes['volume%s.name' % index]
        )
        if status:
            rebalance_status = status.replace(" ", "_")
        else:
            rebalance_status = "not_started"

    volume = NS.gluster.objects.Volume(
        vol_id=volumes['volume%s.id' % index],
        vol_type="arbiter" if int(volumes['volume%s.arbiter_count' % index]) > 0 \
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
        rebal_status=rebalance_status,
    )
    volume.save(ttl=SYNC_TTL)
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
    s_index = 1
    while True:
        try:
            vol_snapshot = NS.gluster.objects.Snapshot(
                vol_id=volumes['volume%s.id' % index],
                id=volumes[
                    'volume%s.snapshot%s.id' % (index, s_index)
                ],
                name=volumes[
                    'volume%s.snapshot%s.name' % (index, s_index)
                ],
                time=volumes[
                    'volume%s.snapshot%s.time' % (index, s_index)
                ],
                description=volumes[
                    'volume%s.snapshot%s.description' % (index, s_index)
                ],
                status=volumes[
                    'volume%s.snapshot%s.status' % (index, s_index)
                ]
            )
            vol_snapshot.save(ttl=SYNC_TTL)
            s_index += 1
        except KeyError:
            break
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
                sbs = NS._int.client.read(
                    "clusters/%s/Bricks/all/"
                    "%s/status" % (
                        NS.tendrl_context.
                        integration_id,
                        brick_name
                    )
                ).value
                current_status = volumes.get(
                    'volume%s.brick%s.status' % (index, b_index)
                )
                if current_status != sbs:
                    msg = "Status of brick: %s " + \
                          "under volume %s chan" + \
                          "ged from %s to %s" % (
                              volumes[ 'volume%s.brick%s' '.path' % (
                                      index,
                                      b_index
                              )],
                              volumes['volume%s.' 'name' % index],
                              sbs,
                              current_status
                          )
                    instance = "volume_%s|brick_%s" % (
                        volumes['volume%s.name' % index],
                        volumes['volume%s.brick%s.path' % (
                            index,
                            b_index
                        )]
                    )
                    self._emit_event(
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
                volumes['volume%s.id' % index ],
                str((b_index - 1) / sub_vol_size),
                brick_name
            )

            NS._int.wclient.write(vol_brick_path, "")

            brick = NS.gluster.objects.Brick(
                    brick_name,
                    vol_id=volumes['volume%s.id' % index],
                    sequence_number=b_index,
                    path=volumes[
                        'volume%s.brick%s.path' % (index, b_index)
                    ],
                    hostname=volumes.get(
                        'volume%s.brick%s.hostname' % (index, b_index)
                    ),
                    port=volumes.get(
                        'volume%s.brick%s.port' % (index, b_index)
                    ),
                    used=True,
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
                    )
                )
            brick.save(ttl=SYNC_TTL)

            # Sync the brick client details
            c_index = 1
            if volumes.get('volume%s.brick%s.client_count' % (index, b_index)) > 0:
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
