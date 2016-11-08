from mock import MagicMock
import sys
sys.modules['logging'] = MagicMock()
sys.modules['tendrl.gluster_bridge.config'] = MagicMock()
from tendrl.gluster_bridge import log
del sys.modules['tendrl.gluster_bridge.config']
del sys.modules['logging']


class Test_log(object):
    def test_log(self):
        log.root = None
        log.setup_logging()
        log.logging.getLogger.assert_called_with('tendrl.gluster_bridge.log')
