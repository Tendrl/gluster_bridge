from tendrl.commons import objects


class Brick(objects.BaseObject):
    def __init__(
        self,
        name,
        disk=None,
        brick_path=None,
        mount_path=None,
        node_id=None,
        vol_id=None,
        sequence_number=None,
        hostname=None,
        port=None,
        status=None,
        filesystem_type=None,
        mount_opts=None,
        utilization=None,
        used=None,
        lv=None,
        vg=None,
        pool=None,
        pv=None,
        size=None,
        disk_type=None,
        disk_count=None,
        stripe_size=None,
        *args,
        **kwargs
    ):
        super(Brick, self).__init__(*args, **kwargs)

        self.disk = disk
        self.name = name
        self.node_id = node_id
        self.brick_path = brick_path
        self.mount_path = mount_path
        self.disk_type = disk_type
        self.disk_count = disk_count
        self.lv = lv
        self.vg = vg
        self.pool = pool
        self.pv = pv
        self.stripe_size = stripe_size
        self.vol_id= vol_id
        self.sequence_number=sequence_number
        self.hostname=hostname
        self.port=port
        self.size=size
        self.status=status
        self.filesystem_type=filesystem_type
        self.mount_opts=mount_opts
        if utilization:
            self.utilization=utilization
        self.used=used
        self.value = 'clusters/{0}/Bricks/all/{1}'

    def render(self):
        self.value = self.value.format(NS.tendrl_context.integration_id,
                                       self.name.replace("/", "_").replace(" ", "")
        )
        return super(Brick, self).render()
