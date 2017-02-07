from tendrl.gluster_integration import flows


class CreateVolume(flows.GlusterIntegrationBaseFlow):
    def run(self):
        super(CreateVolume, self).run()
