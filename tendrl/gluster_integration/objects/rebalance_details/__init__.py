from tendrl.commons import objects


class RebalanceDetails(objects.BaseObject):
    def __init__(
        self,
        vol_id=None,
        node_id=None,
        rebal_data=None,
        rebal_skipped=None,
        rebal_id=None,
        rebal_lookedup=None,
        rebal_files=None,
        rebal_status=None,
        rebal_failures=None,
        *args,
        **kwargs
    ):
        super(RebalanceDetails, self).__init__(*args, **kwargs)

        self.vol_id = vol_id
        self.node_id = node_id
        self.rebal_data=rebal_data
        self.rebal_skipped=rebal_skipped
        self.rebal_id=rebal_id
        self.rebal_lookedup=rebal_lookedup
        self.rebal_files=rebal_files
        self.rebal_status=rebal_status
        self.rebal_failures=rebal_failures
        self.value = 'clusters/{0}/Volumes/{1}/RebalanceDetails/{2}'

    def render(self):
        self.value = self.value.format(NS.tendrl_context.integration_id,
                                       self.vol_id,
                                       self.node_id or NS.node_context.node_id
        )
        return super(RebalanceDetails, self).render()
