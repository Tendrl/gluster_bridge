from tendrl.commons import objects


class Snapshot(objects.BaseObject):
    def __init__(
        self,
        vol_id=None,
        id=None,
        name=None,
        time=None,
        description=None,
        status=None,
        *args,
        **kwargs
    ):
        super(Snapshot, self).__init__(*args, **kwargs)

        self.vol_id = vol_id
        self.id = id
        self.name = name
        self.time = time
        self.description = description
        self.status = status
        self.value = 'clusters/{0}/Volumes/{1}/Snapshots/{2}'

    def render(self):
        self.value = self.value.format(
            NS.tendrl_context.integration_id,
            self.vol_id,
            self.id
        )
        return super(Snapshot, self).render()
