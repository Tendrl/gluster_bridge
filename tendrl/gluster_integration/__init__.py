# flake8: noqa
try:
    from gevent import monkey
except ImportError:
    pass
else:
    monkey.patch_all()

from tendrl.commons import CommonNS

from tendrl.gluster_integration.objects.brick import Brick
from tendrl.gluster_integration.objects.config import Config
from tendrl.gluster_integration.objects.definition import Definition
from tendrl.gluster_integration.objects.node_context import NodeContext
from tendrl.gluster_integration.objects.global_details import GlobalDetails
from tendrl.gluster_integration.objects.peer import Peer
from tendrl.gluster_integration.objects.sync_object import SyncObject
from tendrl.gluster_integration.objects.tendrl_context import TendrlContext
from tendrl.gluster_integration.objects.utilization import Utilization
from tendrl.gluster_integration.objects.volume import Volume
from tendrl.gluster_integration.objects.volume_options import VolumeOptions

from tendrl.gluster_integration.objects.volume.atoms.create import Create
from tendrl.gluster_integration.objects.volume.atoms.delete import Delete
from tendrl.gluster_integration.objects.volume.atoms.start import Start
from tendrl.gluster_integration.objects.volume.atoms.start_rebalance \
    import StartRebalance
from tendrl.gluster_integration.objects.volume.atoms.stop import Stop
from tendrl.gluster_integration.objects.volume.atoms.named_volume_not_exists \
    import NamedVolumeNotExists
from tendrl.gluster_integration.objects.volume.atoms.rebalance_not_running \
    import RebalanceNotRunning
from tendrl.gluster_integration.objects.volume.atoms.volume_exists \
    import VolumeExists
from tendrl.gluster_integration.objects.volume.atoms.volume_not_exists \
    import VolumeNotExists
from tendrl.gluster_integration.objects.volume.atoms.volume_started \
    import VolumeStarted
from tendrl.gluster_integration.objects.volume.atoms.volume_stopped \
    import VolumeStopped

from tendrl.gluster_integration.flows.create_volume import CreateVolume
from tendrl.gluster_integration.objects.volume.flows.delete_volume \
    import DeleteVolume
from tendrl.gluster_integration.objects.volume.flows.start_volume \
    import StartVolume
from tendrl.gluster_integration.objects.volume.flows.start_volume_rebalance \
    import StartVolumeRebalance
from tendrl.gluster_integration.objects.volume.flows.stop_volume \
    import StopVolume


class GlusterIntegrationNS(CommonNS):
    def __init__(self):

        # Create the "tendrl_ns.gluster_integration" namespace
        self.to_str = "tendrl.gluster_integration"
        self.type = 'sds'
        super(GlusterIntegrationNS, self).__init__()

GlusterIntegrationNS()
