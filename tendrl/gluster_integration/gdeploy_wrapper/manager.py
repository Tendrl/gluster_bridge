import importlib
import os
import pkgutil
import re

from tendrl.commons.utils import log_utils as logger
from tendrl.gluster_integration.gdeploy_wrapper import provisioner_base


class ProvisioningManager(object):
    def __init__(self, provisioner):
        self.gluster_provisioner = provisioner
        try:
            self.plugins = []
            self.load_plugins()
        except (SyntaxError, ValueError, ImportError) as ex:
            raise ex
        self.plugin = None
        self.set_plugin()

    def list_modules_in_package_path(self, package_path, prefix):
        modules = []
        path_to_walk = [(package_path, prefix)]
        while len(path_to_walk) > 0:
            curr_path, curr_prefix = path_to_walk.pop()
            for importer, name, ispkg in pkgutil.walk_packages(
                path=[curr_path]
            ):
                if ispkg:
                    path_to_walk.append(
                        (
                            '%s/%s/' % (curr_path, name),
                            '%s.%s' % (curr_prefix, name)
                        )
                    )
                else:
                    modules.append((name, '%s.%s' % (curr_prefix, name)))
        return modules

    def load_plugins(self):
        try:
            path = os.path.dirname(os.path.abspath(__file__)) + '/plugins'
            pkg = 'tendrl.gluster_integration.gdeploy_wrapper.plugins'
            plugins = self.list_modules_in_package_path(path, pkg)
            for name, plugin_fqdn in plugins:
                importlib.import_module(plugin_fqdn)
        except (SyntaxError, ValueError, ImportError) as ex:
            logger.log(
                "debug",
                NS.publisher_id,
                {"message": "Failed to load the gluster provisioner "
                 "plugins. Error %s" % ex},
                integration_id=NS.tendrl_context.integration_id
            )
            raise ex

    def get_plugin(self):
        return self.plugin

    def set_plugin(self):
        for plugin in provisioner_base.ProvisionerBasePlugin.plugins:
            if re.search(self.gluster_provisioner.lower(), type(
                    plugin).__name__.lower(), re.IGNORECASE):
                self.plugin = plugin

    def stop(self):
        self.plugin.destroy()
