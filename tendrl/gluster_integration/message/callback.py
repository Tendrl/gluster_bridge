# The callback function names below should match(in small case)
# the event_type field of gluster native events as documented
# at: https://gluster.readthedocs.io/en/latest/Administrator%20Guide/
# Events%20APIs/ .
# Whenever gluster-integration receives a gluster native event,
# event handler checks if there is a callback function defined for
# that event in this file, If it finds then that callback function
# will be invoked

import etcd
import socket
import subprocess

from tendrl.commons.utils import etcd_utils
from tendrl.commons.utils import log_utils as logger
from tendrl.commons.utils import monitoring_utils
from tendrl.commons.utils import time_utils
from tendrl.gluster_integration import ini2json


import time
import uuid

RESOURCE_TYPE_BRICK = "brick"
RESOURCE_TYPE_PEER = "host"
RESOURCE_TYPE_VOLUME = "volume"


class Callback(object):
    def __init__(self):
        self.sync_interval = NS.config.data.get("sync_interval", 10)
        _cluster = NS.tendrl.objects.Cluster(
            integration_id=NS.tendrl_context.integration_id
        ).load()
        self.cluster_short_name = _cluster.short_name

    def quorum_lost(self, event):
        context = "quorum|" + event['message']['volume']
        message = "Quorum of volume: {0} is lost in cluster {1}".format(
            event['message']['volume'],
            self.cluster_short_name
        )
        native_event = NS.gluster.objects.NativeEvents(
            context,
            message=message,
            severity="warning",
            current_value="quorum_lost",
            tags={"entity_type": RESOURCE_TYPE_VOLUME,
                  "volume_name": event['message']['volume']
                  }
        )
        native_event.save()

    def quorum_regained(self, event):
        context = "quorum|" + event['message']['volume']
        message = "Quorum of volume: {0} is regained in cluster {1}".format(
            event['message']['volume'],
            self.cluster_short_name
        )
        native_event = NS.gluster.objects.NativeEvents(
            context,
            message=message,
            severity="recovery",
            current_value="quorum_gained",
            tags={"entity_type": RESOURCE_TYPE_VOLUME,
                  "volume_name": event['message']['volume']
                  }
        )
        native_event.save()

    def svc_connected(self, event):
        context = "svc_connection|" + "%s_%s" % (
            event['message']['svc_name'],
            NS.node_context.fqdn
        )
        volname = event['message'].get('volume', '')
        if volname:
            context += volname

        message = "Service: {0} is connected on node {1} of " \
            "cluster {2}".format(
                event['message']['svc_name'],
                NS.node_context.fqdn,
                self.cluster_short_name
            )
        native_event = NS.gluster.objects.NativeEvents(
            context,
            message=message,
            severity="recovery",
            current_value="service_connected"
        )
        native_event.save()

    def svc_disconnected(self, event):
        context = "svc_connection|" + "%s_%s" % (
            event['message']['svc_name'],
            NS.node_context.fqdn
        )
        volname = event['message'].get('volume', '')
        if volname:
            context += volname

        message = "Service: {0} is disconnected on node {1} of " \
            "cluster {2}".format(
                event['message']['svc_name'],
                NS.node_context.fqdn,
                self.cluster_short_name
            )
        native_event = NS.gluster.objects.NativeEvents(
            context,
            message=message,
            severity="warning",
            current_value="service_disconnected"
        )
        native_event.save()

    def ec_min_bricks_not_up(self, event):
        context = "ec_min_bricks_up|" + event['message']['subvol']
        message = "Minimum number of bricks not up in EC subvolume" \
                  ": {0} in cluster {1}".format(
                      event['message']['subvol'],
                      self.cluster_short_name
                  )
        volume_name = parse_subvolume(event['message']['subvol'])
        native_event = NS.gluster.objects.NativeEvents(
            context,
            message=message,
            severity="warning",
            current_value="ec_min_bricks_not_up",
            tags={"entity_type": RESOURCE_TYPE_VOLUME,
                  "volume_name": volume_name
                  }
        )
        native_event.save()

    def ec_min_bricks_up(self, event):
        context = "ec_min_bricks_up|" + event['message']['subvol']
        message = "Minimum number of bricks back online " \
                  "in EC subvolume: {0} in cluster {1}".format(
                      event['message']['subvol'],
                      self.cluster_short_name
                  )
        volume_name = parse_subvolume(event['message']['subvol'])
        native_event = NS.gluster.objects.NativeEvents(
            context,
            message=message,
            severity="recovery",
            current_value="ec_min_bricks_up",
            tags={"entity_type": RESOURCE_TYPE_VOLUME,
                  "volume_name": volume_name
                  }
        )
        native_event.save()

    def afr_quorum_met(self, event):
        client_pid = event['message'].get('client-pid', '0')
        if int(client_pid) >= 0:
            context = "afr_quorum_state|" + event['message']['subvol']
            message = "Afr quorum is met for subvolume: {0} in " \
                "cluster {1}".format(
                    event['message']['subvol'],
                    self.cluster_short_name
                )
            volume_name = parse_subvolume(event['message']['subvol'])
            native_event = NS.gluster.objects.NativeEvents(
                context,
                message=message,
                severity="recovery",
                current_value="afr_quorum_met",
                tags={"entity_type": RESOURCE_TYPE_VOLUME,
                      "volume_name": volume_name
                      }
            )
            native_event.save()

    def afr_quorum_fail(self, event):
        client_pid = event['message'].get('client-pid', '0')
        if int(client_pid) >= 0:
            context = "afr_quorum_state|" + event['message']['subvol']
            message = "Afr quorum has failed for subvolume:"\
                      " {0} in cluster {1}".format(
                          event['message']['subvol'],
                          self.cluster_short_name
                      )
            volume_name = parse_subvolume(event['message']['subvol'])
            native_event = NS.gluster.objects.NativeEvents(
                context,
                message=message,
                severity="warning",
                current_value="afr_quorum_failed",
                tags={"entity_type": RESOURCE_TYPE_VOLUME,
                      "volume_name": volume_name
                      }
            )
            native_event.save()

    def afr_subvol_up(self, event):
        """{

               'event': 'AFR_SUBVOL_UP',
               'message': {
                   'subvol': 'glustervol-replicate-0',
                   'client-pid': '0'
               },
               'nodeid': '4fa65c79-8a81-4adc-84b7-0e2e10dba1cf',
               'ts': 1556003999
           }
        """

        client_pid = event['message'].get('client-pid', '0')
        if int(client_pid) >= 0:
            context = "afr_subvol_state|" + event['message']['subvol']
            message = "Afr subvolume: {0} is back up in cluster {1}".format(
                event['message']['subvol'],
                self.cluster_short_name
            )
            volume_name = parse_subvolume(event['message']['subvol'])
            native_event = NS.gluster.objects.NativeEvents(
                context,
                message=message,
                severity="recovery",
                current_value="afr_subvol_up",
                tags={"entity_type": RESOURCE_TYPE_VOLUME,
                      "volume_name": volume_name
                      }
            )
            native_event.save()

    def afr_subvols_down(self, event):
        """{

          'event': 'AFR_SUBVOLS_DOWN',
          'message': {
              'subvol': 'glustervol-replicate-0',
              'client-pid': '0'
          },
          'nodeid': '4fa65c79-8a81-4adc-84b7-0e2e10dba1cf',
          'ts': 1556001153
        }
        """

        client_pid = event['message'].get('client-pid', '0')
        if int(client_pid) >= 0:
            context = "afr_subvol_state|" + event['message']['subvol']
            message = "Afr subvolume: {0} is down in cluster {1}".format(
                event['message']['subvol'],
                self.cluster_short_name
            )
            volume_name = parse_subvolume(event['message']['subvol'])
            native_event = NS.gluster.objects.NativeEvents(
                context,
                message=message,
                severity="warning",
                current_value="afr_subvol_down",
                tags={"entity_type": RESOURCE_TYPE_VOLUME,
                      "volume_name": volume_name
                      }
            )
            native_event.save()

    def unknown_peer(self, event):
        context = "unknown_peer|" + event['message']['peer'].split(":")[0]
        message = "Peer {0} has moved to unknown state in cluster {1}".format(
            event['message']['peer'].split(":")[0],
            self.cluster_short_name
        )
        native_event = NS.gluster.objects.NativeEvents(
            context,
            message=message,
            severity="warning",
            current_value="unknown_peer",
            alert_notify=True
        )
        native_event.save()

    def brickpath_resolve_failed(self, event):
        context = "brickpath_resolve_failed|" + event['message'][
            "peer"] + event['message']["volume"] + event['message']["brick"]
        message = "Brick path resolution failed for brick: {0} . Volume: {1}" \
                  ".Peer: {2} in cluster {3}".format(
                      event['message']["brick"],
                      event['message']["volume"],
                      event['message']['peer'],
                      self.cluster_short_name
                  )
        native_event = NS.gluster.objects.NativeEvents(
            context,
            message=message,
            severity="warning",
            current_value="brick_path_resolve_failed",
            alert_notify=True
        )
        native_event.save()

    def quota_crossed_soft_limit(self, event):
        context = "quota_crossed_soft_limit|" + event[
            "message"]["volume"] + event["message"]["path"]
        message = "Quota soft limit crossed in volume: {0} for path: {1}" \
                  ". Current usage: {2} in cluster {3}".format(
                      event['message']['volume'],
                      event['message']['path'],
                      event['message']['usage'],
                      self.cluster_short_name
                  )
        native_event = NS.gluster.objects.NativeEvents(
            context,
            message=message,
            severity="warning",
            current_value="quota_crossed_soft_limit",
            alert_notify=True
        )
        native_event.save()

    def bitrot_bad_file(self, event):
        context = "bitrot_bad_file|" + event['message'][
            "brick"] + event['message']["path"] + event['message']["gfid"]
        message = "File with gfid: {0} is corrupted due to bitrot." \
                  "  Brick: {1}. Path: {2} in cluster {3}".format(
                      event['message']['gfid'],
                      event['message']['brick'],
                      event['message']['path'],
                      self.cluster_short_name
                  )
        native_event = NS.gluster.objects.NativeEvents(
            context,
            message=message,
            severity="warning",
            current_value="bitrot_bad_file",
            alert_notify=True
        )
        native_event.save()

    def afr_split_brain(self, event):
        context = "afr_split_brain|" + event['message']["subvol"]
        message = "Subvolume: {0} is affected by split-brain. Some of the" \
                  "replicated files in the volume might"\
                  " be divergent in cluster {1}".format(
                      event['message']['subvol'],
                      self.cluster_short_name
                  )
        native_event = NS.gluster.objects.NativeEvents(
            context,
            message=message,
            severity="warning",
            current_value="afr_split_brain",
            alert_notify=True
        )
        native_event.save()

    def snapshot_soft_limit_reached(self, event):
        context = "snapshot_soft_limit_reached|" + event[
            'message']['volume_name']
        message = "Snapshot soft limit reached for"\
                  " volume: {0} in cluster {1}".format(
                      event['message']['volume_name'],
                      self.cluster_short_name
                  )
        native_event = NS.gluster.objects.NativeEvents(
            context,
            message=message,
            severity="warning",
            current_value="snapshot_soft_limit_reached",
            alert_notify=True
        )
        native_event.save()

    def snapshot_hard_limit_reached(self, event):
        context = "snapshot_hard_limit_reached|" + event[
            'message']['volume_name']
        message = "Snapshot hard limit reached for"\
                  " volume: {0} in cluster {1}".format(
                      event['message']['volume_name'],
                      self.cluster_short_name
                  )
        native_event = NS.gluster.objects.NativeEvents(
            context,
            message=message,
            severity="warning",
            current_value="snapshot_hard_limit_reached",
            alert_notify=True
        )
        native_event.save()

    def compare_friend_volume_failed(self, event):
        context = "compare_friend_volume_failed|" + event['message']['volume']
        message = "Compare friend volume failed for volume:"\
                  " {0} in cluster {1}".format(
                      event['message']['volume'],
                      self.cluster_short_name
                  )
        native_event = NS.gluster.objects.NativeEvents(
            context,
            message=message,
            severity="warning",
            current_value="compare_friend_volume_failed",
            alert_notify=True
        )
        native_event.save()

    def posix_health_check_failed(self, event):
        context = "posix_health_check_failed|" + event[
            'message']["brick"] + event['message']["path"]
        message = "Posix health check failed for brick: {0}. Path:"\
                  " {1} in cluster {2}" \
                  ". Error: {3}. op: {4}".format(
                      event['message']["brick"],
                      event['message']["path"],
                      self.cluster_short_name,
                      event['message']["error"],
                      event['message']["op"],
                  )
        native_event = NS.gluster.objects.NativeEvents(
            context,
            message=message,
            severity="warning",
            current_value="posix_health_check_failed",
            alert_notify=True
        )
        native_event.save()

    def peer_reject(self, event):
        context = "peer_reject|" + event['message']['peer'].split(":")[0]
        message = "Peer: {0} is rejected in cluster {1}".format(
            event['message']['peer'].split(":")[0],
            self.cluster_short_name
        )
        native_event = NS.gluster.objects.NativeEvents(
            context,
            message=message,
            severity="warning",
            current_value="peer_reject",
            alert_notify=True
        )
        native_event.save()

    def rebalance_status_update_failed(self, event):
        context = "rebalance_status_update_failed|" + event[
            'message']["volume"]
        message = "Rebalance status update failed for"\
                  " volume: {0} in cluster {1}".format(
                      event['message']["volume"],
                      self.cluster_short_name
                  )
        native_event = NS.gluster.objects.NativeEvents(
            context,
            message=message,
            severity="warning",
            current_value="rebalance_status_update_failed",
            alert_notify=True
        )
        native_event.save()

    def svc_reconfigure_failed(self, event):
        context = "svc_reconfigure_failed|" + event['message']["service"]
        volname = event['message'].get('volume', '')
        if volname:
            context += volname

        message = "Service reconfigure failed for service: {0}".format(
            event['message']['svc_name']
        )
        native_event = NS.gluster.objects.NativeEvents(
            context,
            message=message,
            severity="warning",
            current_value="svc_reconfigure_failed",
            alert_notify=True
        )
        native_event.save()

    def georep_checkpoint_completed(self, event):
        georep_pair = "{0}:{1}:{2}--->{3}:{4}".format(
            event['message']["master_node"],
            event['message']["master_volume"],
            event['message']["brick_path"],
            event['message']["slave_host"],
            event['message']["slave_volume"]
        )
        context = "georep_checkpoint_completed|" + georep_pair
        cp_creation_time = time.localtime(
            float(event['message']['checkpoint_time'])
        )
        cp_creation_time = time.strftime("%d %b %Y %H:%M:%S", cp_creation_time)
        cp_completion_time = time.localtime(
            float(event['message']['checkpoint_completion_time'])
        )
        cp_completion_time = time.strftime(
            "%d %b %Y %H:%M:%S",
            cp_completion_time
        )

        message = "Georeplication checkpoint completed for pair {0}." \
                  " Check point creation time {1}." \
                  " Check point completion time {2}. in cluster {3}".format(
                      georep_pair,
                      cp_creation_time,
                      cp_completion_time,
                      self.cluster_short_name
                  )
        native_event = NS.gluster.objects.NativeEvents(
            context,
            message=message,
            severity="info",
            current_value="georep_checkpoint_completed",
            alert_notify=True
        )
        native_event.save()

    def peer_detach(self, event):
        time.sleep(self.sync_interval)
        job_id = monitoring_utils.update_dashboard(
            event['message']['host'],
            RESOURCE_TYPE_PEER,
            NS.tendrl_context.integration_id,
            "delete"
        )
        logger.log(
            "debug",
            NS.publisher_id,
            {
                "message": "Update dashboard job %s "
                "created" % job_id
            }
        )

    def volume_delete(self, event):
        time.sleep(self.sync_interval)
        fetched_volumes = NS.tendrl.objects.GlusterVolume(
            NS.tendrl_context.integration_id
        ).load_all()
        for fetched_volume in fetched_volumes:
            if fetched_volume.name == event['message']['name']:
                fetched_volume.deleted = True
                fetched_volume.deleted_at = time_utils.now()
                fetched_volume.save()
                try:
                    sub_volumes = etcd_utils.read(
                        "/clusters/{0}/Volumes/{1}/Bricks".format(
                            NS.tendrl_context.integration_id,
                            fetched_volume.vol_id
                        )
                    )

                    for sub_volume in sub_volumes.leaves:
                        bricks = etcd_utils.read(
                            sub_volume.key
                        )
                        for brick in bricks.leaves:
                            fqdn = brick.key.split('/')[-1].split(':')[0]
                            path = brick.key.split('/')[-1].split(':')[-1][1:]
                            # Delete brick dashboard from grafana
                            brick_obj = NS.tendrl.objects.GlusterBrick(
                                NS.tendrl_context.integration_id,
                                fqdn,
                                path
                            ).load()
                            # Delete brick
                            brick_path = "clusters/{0}/Bricks/"\
                                         "all/{1}/{2}".format(
                                             NS.tendrl_context.integration_id,
                                             fqdn,
                                             path
                                         )
                            etcd_utils.delete(
                                brick_path,
                                recursive=True
                            )
                            brick_full_path = fqdn + ":" + brick_obj.\
                                brick_path.split(":")[-1]
                            job_id = monitoring_utils.update_dashboard(
                                "%s|%s" % (
                                    event['message']['name'],
                                    brick_full_path
                                ),
                                RESOURCE_TYPE_BRICK,
                                NS.tendrl_context.integration_id,
                                "delete"
                            )
                            logger.log(
                                "debug",
                                NS.publisher_id,
                                {
                                    "message": "Update dashboard job %s"
                                    " for brick %s "
                                    "in cluster %s created" % (
                                        job_id,
                                        brick.key.split('/')[-1],
                                        NS.tendrl_context.integration_id
                                    )
                                }
                            )
                            # Delete brick from graphite
                            job_id = monitoring_utils.\
                                delete_resource_from_graphite(
                                    "%s|%s" % (
                                        event['message']['name'],
                                        brick_full_path
                                    ),
                                    RESOURCE_TYPE_BRICK,
                                    NS.tendrl_context.integration_id,
                                    "delete"
                                )
                            logger.log(
                                "debug",
                                NS.publisher_id,
                                {
                                    "message": "Delete resource "
                                    "from graphite job %s "
                                    "for brick %s in cluster %s created" % (
                                        job_id,
                                        brick.key.split('/')[-1],
                                        NS.tendrl_context.integration_id
                                    )
                                }
                            )
                except etcd.EtcdKeyNotFound:
                    pass
        # Delete volume dashboard from grafana
        job_id = monitoring_utils.update_dashboard(
            event['message']['name'],
            RESOURCE_TYPE_VOLUME,
            NS.tendrl_context.integration_id,
            "delete"
        )
        logger.log(
            "debug",
            NS.publisher_id,
            {
                "message": "Update dashboard job %s "
                "created" % job_id
            }
        )
        # Delete volume details from graphite
        job_id = monitoring_utils.delete_resource_from_graphite(
            event['message']['name'],
            RESOURCE_TYPE_VOLUME,
            NS.tendrl_context.integration_id,
            "delete"
        )
        logger.log(
            "debug",
            NS.publisher_id,
            {
                "message": "Delete resource from graphite job %s "
                "created" % job_id
            }
        )

    def volume_remove_brick_force(self, event):
        time.sleep(self.sync_interval)
        # Event returns bricks list as space separated single string
        bricks = event['message']['bricks'].split(" ")
        try:
            for brick in bricks:
                # find fqdn using ip
                ip = socket.gethostbyname(brick.split(":/")[0])
                node_id = etcd_utils.read("indexes/ip/%s" % ip).value
                fqdn = NS.tendrl.objects.ClusterNodeContext(
                    node_id=node_id
                ).load().fqdn
                brick = fqdn + ":" + brick.split(":")[-1]
                fetched_brick = NS.tendrl.objects.GlusterBrick(
                    NS.tendrl_context.integration_id,
                    fqdn=brick.split(":/")[0],
                    brick_dir=brick.split(":/")[1].replace('/', '_')
                ).load()

                # delete brick
                etcd_utils.delete(
                    "clusters/{0}/Bricks/all/{1}/{2}".format(
                        NS.tendrl_context.integration_id,
                        brick.split(":/")[0],
                        brick.split(":/")[1].replace('/', '_')
                    ),
                    recursive=True,
                )

                # delete alert dashbaord
                job_id = monitoring_utils.update_dashboard(
                    "%s|%s" % (event['message']['volume'], brick),
                    RESOURCE_TYPE_BRICK,
                    NS.tendrl_context.integration_id,
                    "delete"
                )
                logger.log(
                    "debug",
                    NS.publisher_id,
                    {
                        "message": "Update dashboard job %s "
                        "created" % job_id
                    }
                )

                # delete brick details from graphite
                job_id = monitoring_utils.delete_resource_from_graphite(
                    "%s|%s" % (event['message']['volume'], brick),
                    RESOURCE_TYPE_BRICK,
                    NS.tendrl_context.integration_id,
                    "delete"
                )
                logger.log(
                    "debug",
                    NS.publisher_id,
                    {
                        "message": "Delete resource from graphite job %s "
                        "created" % job_id
                    }
                )

            volume_brick_path = "clusters/{0}/Volumes/{1}/"\
                                "Bricks".format(
                                    NS.tendrl_context.integration_id,
                                    fetched_brick.vol_id,
                                )

            # remove all the brick infromation under volume as the
            # subvolume might have changed, let the next sync handle
            # the updation of brick info
            etcd_utils.delete(
                volume_brick_path,
                recursive=True
            )

            _trigger_sync_key = 'clusters/%s/_sync_now' % \
                NS.tendrl_context.integration_id
            etcd_utils.write(_trigger_sync_key, 'true')
            etcd_utils.refresh(_trigger_sync_key, self.sync_interval)
        except etcd.EtcdKeyNotFound:
            logger.log(
                "debug",
                NS.publisher_id,
                {
                    "message": "Unable to delete bricks %s" % bricks
                }
            )

    def volume_remove_brick_commit(self, event):
        self.volume_remove_brick_force(event)

    def brick_replace(self, event):
        message = event["message"]
        # Remove source brick
        # changing dict keys based on brick function
        brick_details = {}
        brick_details["volume"] = message["Volume"]
        brick_details["bricks"] = message["source-brick"]
        event["message"] = brick_details
        self.volume_remove_brick_force(event)

    def snapshot_restored(self, event):
        time.sleep(self.sync_interval)
        message = event["message"]
        volume = message['volume_name']
        volume_id = ""
        bricks_to_remove = []

        # get the list of current bricks by running get-state
        output_dir = '/var/run/'
        output_file = 'glusterd-state-snapshot-%s' % str(uuid.uuid4())
        subprocess.call(
            [
                'gluster',
                'get-state',
                'glusterd',
                'odir',
                output_dir,
                'file',
                output_file,
                'detail'
            ]
        )
        raw_data = ini2json.ini_to_dict(
            output_dir + output_file
        )
        subprocess.call(['rm', '-rf', output_dir + output_file])
        index = 1
        while True:
            try:
                current_vol = 'volume%s.name' % index
                if raw_data['Volumes'][current_vol] == volume:
                    current_vol_id = 'volume%s.id' % index
                    volume_id = raw_data['Volumes'][current_vol_id]
                    break
            except KeyError:
                return
            index += 1
        latest_bricks = []
        b_index = 1
        while True:
            try:
                curr_brick = 'volume%s.brick%s.path' % (
                    index, b_index
                )
                brick = raw_data['Volumes'][curr_brick]
                b_index += 1
            except KeyError:
                break
            latest_bricks.append(brick)

        # get the list of bricks in etcd for this volume

        sub_volumes = etcd_utils.read(
            "/clusters/{0}/Volumes/{1}/Bricks".format(
                NS.tendrl_context.integration_id,
                volume_id
            )
        )
        for sub_volume in sub_volumes.leaves:
            bricks = etcd_utils.read(
                sub_volume.key
            )
            for brick in bricks.leaves:
                fqdn = brick.key.split('/')[-1].split(':')[0]
                path = brick.key.split('/')[-1].split(':')[-1][1:]

                brick_path = "clusters/{0}/Bricks/"\
                             "all/{1}/{2}".format(
                                 NS.tendrl_context.integration_id,
                                 fqdn,
                                 path
                             )
                brick_full_path = etcd_utils.read(
                    "%s/brick_path" % brick_path
                ).value
                if brick_full_path not in latest_bricks:
                    bricks_to_remove.append(brick_full_path)

        brick_details = {}
        brick_details["volume"] = volume
        brick_details["bricks"] = " ".join(bricks_to_remove)
        event["message"] = brick_details
        self.volume_remove_brick_force(event)


def parse_subvolume(subvol):
    # volume1-replica-2 or volume_1-replica-2 or volume-1-replica-2
    return subvol.split("-" + "-".join(subvol.split('-')[-2:]))[0]
