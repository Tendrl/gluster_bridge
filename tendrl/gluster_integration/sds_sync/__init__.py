import json
import logging
import os
import re

import etcd
import gevent
import subprocess

from tendrl.commons import sds_sync
from tendrl.gluster_integration import ini2json
from gstatus.libgluster.cluster import Cluster
from gstatus.libutils import utils as status_utils

LOG = logging.getLogger(__name__)


class GlusterIntegrationSdsSyncStateThread(sds_sync.SdsSyncThread):

    def __init__(self):
        super(GlusterIntegrationSdsSyncStateThread, self).__init__()
        self._complete = gevent.event.Event()

    def _run(self):
        LOG.info("%s running" % self.__class__.__name__)

        while not self._complete.is_set():
            try:
                gevent.sleep(3)
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
                NS.sync_object = NS.gluster_integration.objects.\
                    SyncObject(data=json.dumps(raw_data))
                NS.sync_object.save()

                if "Peers" in raw_data:
                    index = 1
                    peers = raw_data["Peers"]
                    while True:
                        try:
                            NS.peer = NS.gluster_integration.\
                                objects.Peer(
                                    peer_uuid=peers['peer%s.uuid' % index],
                                    hostname=peers['peer%s.primary_hostname' % index],
                                    state=peers['peer%s.state' % index]
                                )
                            NS.peer.save()
                            index += 1
                        except KeyError:
                            break
                if "Volumes" in raw_data:
                    index = 1
                    volumes = raw_data['Volumes']
                    while True:
                        try:
                            NS.volume = NS.gluster_integration.objects.Volume(
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
                            NS.volume.save()
                            b_index = 1
                            while True:
                                try:
                                    NS.brick = NS.gluster_integration\
                                        .objects.Brick(
                                            vol_id=volumes['volume%s.id' % index],
                                            path=volumes[
                                                'volume%s.brick%s.path' % (
                                                    index, b_index)],
                                            hostname=volumes.get(
                                                'volume%s.brick%s.hostname' % (
                                                    index, b_index)),
                                            port=volumes.get(
                                                'volume%s.brick%s.port' % (
                                                    index, b_index)),
                                            status=volumes.get(
                                                 'volume%s.brick%s.status' % (
                                                    index, b_index)),
                                            filesystem_type=volumes.get(
                                                'volume%s.brick%s.filesystem_type' % (
                                                    index, b_index)),
                                            mount_opts=volumes.get(
                                                'volume%s.brick%s.mount_options' % (
                                                    index, b_index))
                                        )
                                    NS.brick.save()
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
                        NS.vol_options = NS.gluster_integration.objects.\
                            VolumeOptions(
                                vol_id=vol_id,
                                options=dict1
                            )
                        NS.vol_options.save()

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
                    NS.gluster_integration.objects.GlobalDetails(
                        status=out_dict['status']
                    ).save()
                    NS.gluster_integration.objects.Utilization(
                        raw_capacity=out_dict['raw_capacity'],
                        usable_capacity=out_dict['usable_capacity'],
                        used_capacity=out_dict['used_capacity'],
                        pcnt_used=(out_dict['used_capacity'] * 100 / out_dict['usable_capacity'])
                    ).save()
                    for item in out_dict['volume_summary']:
                        NS.gluster_integration.objects.Volume(
                            name=item['volume_name'],
                            usable_capacity=item['usable_capacity'],
                            used_capacity=item['used_capacity'],
                            pcnt_used=(item['used_capacity'] * 100 / item['usable_capacity'])
                        ).save()

            except Exception as ex:
                LOG.error(ex)

        LOG.info("%s complete" % self.__class__.__name__)

