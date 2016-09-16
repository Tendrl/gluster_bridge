import os
import ConfigParser

class ConfigNotFound(Exception):
    pass


DEFAULT_CONFIG_PATH = "/etc/tendrl/gluster_bridge.conf"
CONFIG_PATH_VAR = "TENDRL_CONFIG"


class TendrlConfig(ConfigParser.SafeConfigParser):
    def __init__(self):
        ConfigParser.SafeConfigParser.__init__(self)

        try:
            self.path = os.environ[CONFIG_PATH_VAR]
        except KeyError:
            self.path = DEFAULT_CONFIG_PATH

        if not os.path.exists(self.path):
            raise ConfigNotFound("Tendrl Configuration not found at %s" % self.path)

        self.read(self.path)
