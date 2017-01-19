from tendrl.commons.persistence.etcd_persister import EtcdPersister
from tendrl.gluster_integration.persistence.sync_objects import SyncObject


class GlusterIntegrationEtcdPersister(EtcdPersister):
    def __init__(self, etcd_orm):
        super(GlusterIntegrationEtcdPersister, self).__init__(etcd_orm)

    def update_sync_object(self, updated, cluster_id, data):
        self.etcd_orm.save(
            SyncObject(
                updated=updated,
                cluster_id=cluster_id,
                data=data
            )
        )

    def update_peer(self, peer):
        self.etcd_orm.save(peer)

    def update_volume(self, vol):
        self.etcd_orm.save(vol)

    def update_brick(self, brick):
        self.etcd_orm.save(brick)

    def update_volume_options(self, vol_options):
        self.etcd_orm.save(vol_options)

    def save_events(self, events):
        for event in events:
            self.etcd_orm.save(event)

    def update_tendrl_context(self, context):
        self.etcd_orm.save(context)

    def update_tendrl_definitions(self, definition):
        self.etcd_orm.save(definition)
