from tendrl.commons import objects


class Volume(objects.BaseObject):
    def __init__(
        self,
        vol_id=None,
        vol_type=None,
        name=None,
        status=None,
        state=None,
        brick_count=None,
        deleted=None,
        transport_type=None,
        snap_count=None,
        stripe_count=None,
        replica_count=None,
        subvol_count=None,
        arbiter_count=None,
        disperse_count=None,
        redundancy_count=None,
        quorum_status=None,
        snapd_status=None,
        snapd_inited=None,
        rebal_status=None,
        usable_capacity=None,
        used_capacity=None,
        pcnt_used=None,
        profiling_enabled=None,
        client_count=None,
        rebal_estimated_time=None,
        options=None,
        *args,
        **kwargs
    ):
        super(Volume, self).__init__(*args, **kwargs)

        self.vol_id = vol_id
        self.vol_type = vol_type
        self.name = name
        self.status = status
        self.state = state
        self.brick_count = brick_count
        self.deleted = deleted
        self.transport_type = transport_type
        self.snap_count = snap_count
        self.stripe_count = stripe_count
        self.replica_count = replica_count
        self.subvol_count = subvol_count
        self.arbiter_count = arbiter_count
        self.disperse_count = disperse_count
        self.redundancy_count = redundancy_count
        self.quorum_status = quorum_status
        self.snapd_status = snapd_status
        self.snapd_inited = snapd_inited
        self.rebal_status = rebal_status
        self.usable_capacity = usable_capacity
        self.used_capacity = used_capacity
        self.pcnt_used = pcnt_used
        self.profiling_enabled = profiling_enabled
        self.client_count = client_count
        self.rebal_estimated_time = rebal_estimated_time
        self.options = options
        self.value = 'clusters/{0}/Volumes/{1}'

    def render(self):
        self.value = self.value.format(NS.tendrl_context.integration_id,
                                       self.vol_id)
        return super(Volume, self).render()
