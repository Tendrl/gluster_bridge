from tendrl.commons.persistence.etcd_persister import EtcdPersister
from tendrl.gluster_integration.persistence.sync_objects import SyncObject


class GlusterIntegrationEtcdPersister(EtcdPersister):
    def __init__(self, config):
        super(GlusterIntegrationEtcdPersister, self).__init__(config)
        self._store = self.get_store()

    def update_sync_object(self, updated, cluster_id, data):
        self._store.save(
            SyncObject(
                updated=updated,
                cluster_id=cluster_id,
                data=data
            )
        )

    def update_peer(self, peer):
        self._store.save(peer)

    def update_volume(self, vol):
        self._store.save(vol)

    def update_brick(self, brick):
        self._store.save(brick)

    def save_events(self, events):
        for event in events:
            self._store.save(event)

    def update_tendrl_context(self, context):
        self._store.save(context)

    def update_tendrl_definitions(self, definition):
        self._store.save(definition)
