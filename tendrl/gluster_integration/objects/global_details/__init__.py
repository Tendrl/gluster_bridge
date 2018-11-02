from tendrl.commons import objects
from tendrl.commons.utils import etcd_utils
from tendrl.commons.utils import event_utils


class GlobalDetails(objects.BaseObject):
    def __init__(self, status=None, connection_count=0,
                 connection_active=0, volume_up_degraded=0,
                 peer_count=0, vol_count=0, integration_id=None,
                 *args, **kwargs):
        super(GlobalDetails, self).__init__(*args, **kwargs)

        self.status = status
        self.connection_count = connection_count
        self.connection_active = connection_active
        self.volume_up_degraded = volume_up_degraded
        self.peer_count = peer_count
        self.vol_count = vol_count
        self.integration_id = integration_id
        self.value = 'clusters/{0}/GlobalDetails'

    def render(self):
        self.value = self.value.format(
            self.integration_id or NS.tendrl_context.integration_id)
        return super(GlobalDetails, self).render()

    def save(self, update=True, ttl=None):
        self.invalidate_hash()
        super(GlobalDetails, self).save(update)
        status = self.value + "/status"
        if ttl:
            etcd_utils.refresh(status, ttl)

    def on_change(self, attr, prev_value, current_value):
        if attr == "status":
            self.on_change_status(prev_value, current_value)

    def on_change_status(self, prev_value, current_value):
        if current_value is None:
            self.status = "unhealthy"
            self.save()

            _ctc = \
                NS.tendrl.objects.ClusterTendrlContext(
                    integration_id=self.integration_id
                ).load()

            msg = "Cluster {0} moved to unhealthy state".format(
                _ctc.cluster_name
            )
            event_utils.emit_event(
                "cluster_health_status",
                "unhealthy",
                msg,
                "cluster_{0}".format(
                    _ctc.integration_id
                ),
                "WARNING",
                integration_id=_ctc.integration_id,
                cluster_name=_ctc.cluster_name,
                sds_name=_ctc.sds_name
            )
