from tendrl.commons import central_store


class GlusterIntegrationEtcdCentralStore(central_store.EtcdCentralStore):
    def __init__(self):
        super(GlusterIntegrationEtcdCentralStore, self).__init__()

    def save_syncobject(self, sync_object):
        tendrl_ns.etcd_orm.save(sync_object)

    def save_globaldetails(self, details):
        tendrl_ns.etcd_orm.save(details)

    def save_config(self, config):
        tendrl_ns.etcd_orm.save(config)

    def save_definition(self, definition):
        tendrl_ns.etcd_orm.save(definition)

    def save_peer(self, peer):
        tendrl_ns.etcd_orm.save(peer)

    def save_volume(self, vol):
        tendrl_ns.etcd_orm.save(vol)

    def save_brick(self, brick):
        tendrl_ns.etcd_orm.save(brick)

    def save_volumeoptions(self, vol_options):
        tendrl_ns.etcd_orm.save(vol_options)

    def save_events(self, events):
        for event in events:
            tendrl_ns.etcd_orm.save(event)

    def save_tendrl_definitions(self, definition):
        tendrl_ns.etcd_orm.save(definition)

    def save_tendrlcontext(self, tendrl_context):
        tendrl_ns.etcd_orm.save(tendrl_context)

    def save_nodecontext(self, node_context):
        tendrl_ns.etcd_orm.save(node_context)

    def save_utilization(self, utilization):
        tendrl_ns.etcd_orm.save(utilization)

    def save_job(self, job):
        tendrl_ns.etcd_orm.save(job)
