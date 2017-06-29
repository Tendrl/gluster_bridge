import etcd
import gevent
import json
import os
import re
import socket
import subprocess

from tendrl.gluster_integration.sds_sync import brick_utilization
from tendrl.gluster_integration.sds_sync import rebalance_status as rebal_stat
from tendrl.commons.event import Event
from tendrl.commons.message import ExceptionMessage, Message
from tendrl.commons.utils import cmd_utils
from tendrl.commons.utils import etcd_utils

from tendrl.commons import sds_sync
from tendrl.gluster_integration import ini2json
from tendrl.commons.utils.time_utils import now as tendrl_now


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

        gluster_brick_dir = NS.gluster.objects.\
                            GlusterBrickDir()
        gluster_brick_dir.save()

        try:
            etcd_utils.read(
                "clusters/%s/cluster_network" % NS.tendrl_context.integration_id
            )
        except etcd.EtcdKeyNotFound:
            try:
                node_networks = etcd_utils.read(
                    "nodes/%s/Networks" % NS.node_context.node_id
                )
                # TODO: (team) this logic needs to change later
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
                        payload={"message": "Failed to sync cluster network details"}
                    )
                )
                raise ex

        while not self._complete.is_set():
            try:
                gevent.sleep(
                    int(NS.config.data.get("sync_interval", 10))
                )
                NS._int.wclient.write("clusters/%s/sync_status" % NS.tendrl_context.integration_id,
                                      "in_progress", prevExist=False)
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
                        'glusterd-state'
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
                                    hostname=peers['peer%s.primary_hostname' % index],
                                    state=peers['peer%s.state' % index]
                                )
                            peer.save(ttl=SYNC_TTL)
                            index += 1
                        except KeyError:
                            break
                if "Volumes" in raw_data:
                    index = 1
                    volumes = raw_data['Volumes']
                    node_context = NS.node_context.load()
                    tag_list = list(node_context.tags)
                    while True:
                        try:
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
                                    if current_status != stored_volume_status:
                                        msg = "Status of volume: %s changed from %s to %s" % (
                                            volumes['volume%s.name' % index],
                                            stored_volume_status,
                                            current_status
                                        )
                                        instance = "volume_%s" % volumes['volume%s.name' % index]
                                        self._emit_event(
                                            "volume_status",
                                            current_status,
                                            msg,
                                            instance
                                        )                                        
                                    
                                except etcd.EtcdKeyNotFound:
                                    pass

                            rebalance_status = ""
                            if volumes[
                                    'volume%s.type' % index
                            ].startswith("Distribute"):
                                status = rebal_stat.get_rebalance_status(volumes[
                                    'volume%s.name' % index
                                ])
                                if status:
                                    rebalance_status = status.replace(" ", "_")
                                else:
                                    rebalance_status = "not_started"
                            
                            volume = NS.gluster.objects.Volume(
                                vol_id=volumes[
                                    'volume%s.id' % index
                                ],
                                vol_type=volumes[
                                    'volume%s.type' % index
                                ],
                                name=volumes[
                                    'volume%s.name' % index
                                ],
                                transport_type=volumes[
                                    'volume%s.transport_type' % index
                                ],
                                status=volumes[
                                    'volume%s.status' % index
                                ],
                                brick_count=volumes[
                                    'volume%s.brickcount' % index
                                ],
                                snap_count=volumes[
                                    'volume%s.snap_count' % index
                                ],
                                stripe_count=volumes[
                                    'volume%s.stripe_count' % index
                                ],
                                replica_count=volumes[
                                    'volume%s.replica_count' % index
                                ],
                                subvol_count=volumes[
                                    'volume%s.subvol_count' % index
                                ],
                                arbiter_count=volumes[
                                    'volume%s.arbiter_count' % index
                                ],
                                disperse_count=volumes[
                                    'volume%s.disperse_count' % index
                                 ],
                                redundancy_count=volumes[
                                    'volume%s.redundancy_count' % index
                                ],
                                quorum_status=volumes[
                                    'volume%s.quorum_status' % index
                                ],
                                snapd_status=volumes[
                                    'volume%s.snapd_svc.online_status' % index
                                ],
                                snapd_inited=volumes[
                                    'volume%s.snapd_svc.inited' % index
                                ],
                                rebal_status=rebalance_status,
                            )
                            volume.save(ttl=SYNC_TTL)
                            rebalance_details = NS.gluster.objects.RebalanceDetails(
                                vol_id=volumes[
                                    'volume%s.id' % index
                                ],
                                rebal_id=volumes[
                                    'volume%s.rebalance.id' % index
                                ],
                                rebal_status=volumes[
                                    'volume%s.rebalance.status' % index
                                ],
                                rebal_failures=volumes[
                                    'volume%s.rebalance.failures' % index
                                ],
                                rebal_skipped=volumes[
                                    'volume%s.rebalance.skipped' % index
                                ],
                                rebal_lookedup=volumes[
                                    'volume%s.rebalance.lookedup' % index
                                ],
                                rebal_files=volumes[
                                    'volume%s.rebalance.files' % index
                                ],
                                rebal_data=volumes[
                                    'volume%s.rebalance.data' % index
                                ],
                            )
                            rebalance_details.save(ttl=SYNC_TTL)
                            b_index = 1
                            # ipv4 address of current node
                            try:
                                network_ip = []
                                networks = NS._int.client.read(
                                    "nodes/%s/Networks" % NS.node_context.node_id
                                )
                                for interface in networks.leaves:
                                    key = interface.key.split("/")[-1]
                                    network = NS.tendrl.objects.NodeNetwork(
                                        interface=key).load()
                                    network_ip.extend(network.ipv4)
                            except etcd.EtcdKeyNotFound as ex:
                                Event(
                                    ExceptionMessage(
                                        priority="debug",
                                        publisher=NS.publisher_id,
                                        payload={"message": "Could not find any ipv4 networks for node %s" % NS.node_context.node_id,
                                                 "exception": ex
                                                 }
                                    )
                                )
                            while True:
                                try:
                                    # Update brick node wise
                                    hostname  = volumes['volume%s.brick%s.hostname' % (
                                        index, b_index)]
                                    if (NS.node_context.fqdn != hostname) and (
                                        hostname not in network_ip):
                                        b_index += 1
                                        continue
                                    sub_vol_size = (int(volumes['volume%s.brickcount' % index])) / int(
                                        volumes[
                                            'volume%s.subvol_count' % index
                                        ]
                                    )
                                    brick_name = NS.node_context.fqdn + ":" + volumes[
                                        'volume%s.brick%s.path' % (
                                            index, b_index
                                        )
                                    ].split(":")[-1].replace("/","_")

                                    # Raise alerts if the brick path changes
                                    try:
                                        stored_brick_status = NS._int.client.read(
                                            "clusters/%s/Bricks/all/%s/status" % (
                                                NS.tendrl_context.integration_id,
                                                brick_name
                                            )
                                        ).value
                                        current_status = volumes.get(
                                            'volume%s.brick%s.status' % (
                                                index,
                                                b_index
                                            )
                                        )
                                        if current_status != stored_brick_status:
                                            msg = "Status of brick: %s under volume %s changed from %s to %s" % (
                                                volumes[
                                                    'volume%s.brick%s.path' % (
                                                        index, b_index
                                                    )
                                                ],
                                                volumes['volume%s.name' % index],
                                                stored_brick_status,
                                                current_status
                                            )
                                            instance = "volume_%s|brick_%s" % (
                                                volumes['volume%s.name' % index],
                                                volumes[
                                                    'volume%s.brick%s.path' % (
                                                        index, b_index
                                                    )
                                                ]
                                            )
                                            self._emit_event(
                                                "brick_status",
                                                current_status,
                                                msg,
                                                instance
                                            )                                        
                                    
                                    except etcd.EtcdKeyNotFound:
                                        pass

                                    vol_brick_path = "clusters/%s/Volumes/%s/Bricks/subvolume%s/%s" % (
                                        NS.tendrl_context.integration_id,
                                        volumes['volume%s.id' % index],
                                        str((b_index - 1) / sub_vol_size),
                                        brick_name
                                    )

                                    NS._int.wclient.write(
                                        vol_brick_path,
                                        ""
                                    )

                                    brick = NS.gluster\
                                        .objects.Brick(
                                            brick_name,
                                            vol_id=volumes['volume%s.id' % index],
                                            sequence_number=b_index, 
                                            path=volumes[
                                                'volume%s.brick%s.path' % (
                                                    index, b_index)],
                                            hostname=volumes.get(
                                                'volume%s.brick%s.hostname' % (
                                                    index, b_index)),
                                            port=volumes.get(
                                                'volume%s.brick%s.port' % (
                                                    index, b_index)),
                                            used=True,
                                            status=volumes.get(
                                                 'volume%s.brick%s.status' % (
                                                    index, b_index)),
                                            filesystem_type=volumes.get(
                                                'volume%s.brick%s.filesystem_type' % (
                                                    index, b_index)),
                                            mount_opts=volumes.get(
                                                'volume%s.brick%s.mount_options' % (
                                                    index, b_index)),
                                            utilization=brick_utilization\
                                                .brick_utilization(
                                                    volumes['volume%s.brick%s.path' % (
                                                        index, b_index)])
                                        )
                                    brick.save(ttl=SYNC_TTL)

                                    b_index += 1
                                except KeyError:
                                    break
                            index += 1
                        except KeyError:
                            break
                    # poplate the volume options
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

                # Sync the cluster status details
                args = ["gstatus", "-o", "json"]
                p = subprocess.Popen(
                    args,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    stdin=open(os.devnull, "r")
                )
                stdout, stderr = p.communicate()
                if stderr == "" and p.returncode == 0:
                    # The format of the output from gstatus is as below

                    # "2017-02-23 18:13:01.944183 {"brick_count": 2,
                    # bricks_active": 2, "glfs_version": "3.9.0",
                    # node_count": 2, "nodes_active": 2, "over_commit": "No",
                    # product_name": "Community", "raw_capacity": 52181536768,
                    # sh_active": 0, "sh_enabled": 0, "snapshot_count": 0,
                    # status": "healthy", "usable_capacity": 52181536768,
                    # used_capacity": 2117836800, "volume_count": 1,
                    # volume_summary": [{"snapshot_count": 0, "state": "up",
                    # usable_capacity": 52181536768, "used_capacity": 2117836800,
                    # volume_name": "vol1"}]}\n"

                    out_dict = json.loads(stdout[stdout.index('{'): -1])
                    NS.gluster.objects.Utilization(
                        raw_capacity=out_dict['raw_capacity'],
                        usable_capacity=out_dict['usable_capacity'],
                        used_capacity=out_dict['used_capacity'],
                        pcnt_used=(out_dict['used_capacity'] * 100 / out_dict['usable_capacity'])
                    ).save()
                    volume_up_degraded = 0
                    for item in out_dict['volume_summary']:
                        if "up(degraded)" in item['state']:
                            volume_up_degraded = volume_up_degraded + 1
                        volumes = NS._int.client.read(
                            "clusters/%s/Volumes" % NS.tendrl_context.integration_id
                        )
                        for child in volumes._children:
                            volume = NS.gluster.objects.Volume(
                                vol_id=child['key'].split('/')[-1]
                            ).load()
                            if volume.name == item['volume_name']:
                                NS.gluster.objects.Volume(
                                    vol_id=child['key'].split('/')[-1],
                                    usable_capacity=str(item['usable_capacity']),
                                    used_capacity=str(item['used_capacity']),
                                    pcnt_used=str(item['used_capacity'] * 100 / item['usable_capacity']),
                                    vol_type=volume.vol_type,
                                    name=volume.name,
                                    transport_type=volume.transport_type,
                                    status=volume.status,
                                    brick_count=volume.brick_count,
                                    snap_count=volume.snap_count,
                                    stripe_count=volume.stripe_count,
                                    replica_count=volume.replica_count,
                                    subvol_count=volume.subvol_count,
                                    arbiter_count=volume.arbiter_count,
                                    disperse_count=volume.disperse_count,
                                    redundancy_count=volume.redundancy_count,
                                    quorum_status=volume.quorum_status,
                                    snapd_status=volume.snapd_status,
                                    snapd_inited=volume.snapd_inited,
                                    rebal_status=volume.rebal_status,
                                ).save()
                    connection_count = None
                    connection_active  = None
                    # gstatus result:
                    # Product: Community          Capacity:  25.00 GiB(raw bricks)
                    # Status: HEALTHY                        3.00 GiB(raw used)
                    # Glusterfs: 3.9.0                         87.00 GiB(usable from volumes)
                    # OverCommit: Yes               Snapshots:   0
                    # Nodes       :  2/  2		  Volumes:   4 Up
                    # Self Heal   :  0/  0		             0 Up(Degraded)
                    # Bricks      :  7/  7		             0 Up(Partial)
                    # Connections :  1/   2                     0 Down

                    # grep result:
                    # Connections :  0/   0        1 Down
                    cmd = cmd_utils.Command('gstatus -s | grep Connections', True)
                    out, err, rc = cmd.run()
                    if not err:
                        if "Connection" in out:
                            out = (re.split(r'\s{5,}', out)[0])
                            out = out.split(":")[1].replace(" ", "").split("/")
                            connection_active  = int(out[0])
                            connection_count = int(out[1])
                    NS.gluster.objects.GlobalDetails(
                        status=out_dict['status'],
                        volume_up_degraded=volume_up_degraded,
                        connection_active=connection_active,
                        connection_count=connection_count
                    ).save()

                else:
                    # If gstatus does not return any status details in absence
                    # of volumes, default set the status and utilization details
                    # A sample output from gstatus if no volumes in cluster is as below
                    #
                    # >gstatus -o json
                    # This cluster doesn't have any volumes/daemons running.
                    # The output below shows the current nodes attached to this host.
                    #
                    # UUID					Hostname    	State
                    # 6a48d2f7-3859-4542-86e0-0a8146588f31	{FQDN-1}	Connected
                    # 7380e303-83b6-4728-918f-e99029bc1bce	{FQDN-2}	Connected
                    # 751ecb42-da85-4c3d-834d-9824d1ce7fd3	{FQDN-3}	Connected
                    # 388b708c-a86c-4a16-9a6f-f0d53ea79a51	localhost   	Connected

                    out_lines = stdout.split('\n')
                    connected = True
                    for index in range(4, len(out_lines) - 2):
                        node_status_det = out_lines[index].split('\t')
                        if len(node_status_det) > 2:
                            if node_status_det[2].strip() != 'Connected':
                                connected = connected and False
                    if connected:
                        NS.gluster.objects.GlobalDetails(
                            status='healthy'
                        ).save()
                    else:
                        NS.gluster.objects.GlobalDetails(
                            status='unhealthy'
                        ).save()
                    NS.gluster.objects.Utilization(
                        raw_capacity=0,
                        usable_capacity=0,
                        used_capacity=0,
                        pcnt_used=0
                    ).save()
            _cluster = NS.tendrl.objects.Cluster(integration_id=NS.tendrl_context.integration_id)
            if _cluster.exists():
                try:
                    _cluster.sync_status = "done"
                    _cluster.last_sync = str(tendrl_now())
                    _cluster.save()

                except Exception as ex:
                    Event(
                        ExceptionMessage(
                            priority="error",
                            publisher=NS.publisher_id,
                            payload={"message": "gluster sds state sync error",
                                     "exception": ex}
                        )
                    )
		    # raise ex?

        Event(
            Message(
                priority="debug",
                publisher=NS.publisher_id,
                payload={"message": "%s complete" % self.__class__.__name__}
            )
        )

