from tendrl.commons.config import load_config
from tendrl.commons.etcdobj.etcdobj import Server as etcd_server
from tendrl.commons.persistence.etcd_persister import EtcdPersister
from tendrl.gluster_integration.persistence.sync_objects import SyncObject

config = load_config(
    "gluster-integration",
    "/etc/tendrl/tendrl.conf"
)


class GlusterIntegrationEtcdPersister(EtcdPersister):
    def __init__(self):
        etcd_kwargs = {
            'port': int(config.get("commons", "etcd_port")),
            'host': config.get("commons", "etcd_connection")
        }
        self.server = etcd_server(etcd_kwargs=etcd_kwargs)
        super(GlusterIntegrationEtcdPersister, self).__init__()

    def update_sync_object(self, updated, cluster_id, data):
        self.server.save(
            SyncObject(
                updated=updated,
                cluster_id=cluster_id,
                data=data
            )
        )

    def update_peer(self, peer):
        self.server.save(peer)

    def update_volume(self, vol):
        self.server.save(vol)

    def update_brick(self, brick):
        self.server.save(brick)

    def update_volume_options(self, vol_options):
        self.server.save(vol_options)

    def save_events(self, events):
        for event in events:
            self.server.save(event)

    def update_tendrl_context(self, context):
        self.server.save(context)

    def update_tendrl_definitions(self, definition):
        self.server.save(definition)
