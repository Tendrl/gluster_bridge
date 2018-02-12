import __builtin__
import importlib
import etcd
import maps
import mock
from mock import patch
import pytest
import inspect
import maps
import mock

from tendrl.gluster_integration.objects.utilization import Utilization
from tendrl.gluster_integration.objects.volume import Volume
from tendrl.gluster_integration.sds_sync import utilization


from tendrl.commons.flows.exceptions import FlowExecutionFailedError
from tendrl.commons.flows.import_cluster import ImportCluster
from tendrl.commons import objects
from tendrl.gluster_integration.objects.volume import Volume
from tendrl.commons.objects import AtomExecutionFailedError
from tendrl.commons.objects.node.atoms.cmd import Cmd
import tendrl.commons.objects.node_context as node
from tendrl.commons import TendrlNS

from tendrl.gluster_integration.sds_sync import GlusterIntegrationSdsSyncStateThread


@patch.object(etcd, "Client")
@patch.object(etcd.Client, "read")
@patch.object(node.NodeContext, '_get_node_id')
def init(patch_get_node_id, patch_read, patch_client):
    patch_get_node_id.return_value = 1
    patch_read.return_value = etcd.Client()
    patch_client.return_value = etcd.Client()
    setattr(__builtin__, "NS", maps.NamedDict())
    setattr(NS, "_int", maps.NamedDict())
    NS._int.etcd_kwargs = {
        'port': 1,
        'host': 2,
        'allow_reconnect': True}
    NS._int.client = etcd.Client(**NS._int.etcd_kwargs)
    NS._int.wclient = etcd.Client(**NS._int.etcd_kwargs)
    NS["config"] = maps.NamedDict()
    NS.config["data"] = maps.NamedDict()
    NS.config.data['tags'] = "test"
    NS.publisher_id = "node_context"
    NS.config.data['etcd_port'] = 8085
    NS.config.data['etcd_connection'] = "Test Connection"
    tendrlNS = TendrlNS()
    return tendrlNS


def return_fail(param):
    return NS.tendrl.objects.Cluster(
        integration_id='13ced2a7-cd12-4063-bf6c-a8226b0789a0',
        import_status='done',
        import_job_id='0f2381f0-e6e3-4cad-bb84-47c06cb46ffb'
    )


def return_pass(param):
    return NS.tendrl.objects.Cluster(
        integration_id='13ced2a7-cd12-4063-bf6c-a8226b0789a0',
        import_status='new',
        import_job_id=''
    )


def read(key):
    if key == 'indexes/tags/tendrl/integration/None':
        raise etcd.EtcdKeyNotFound
    else:
        return maps.NamedDict(
            value=u'["bc15f88b-7118-485e-ab5c-cf4b9e1c2ee5"]'
        )


def save(*args):
    pass



def get_obj_definition(*args, **kwargs):
    ret = maps.NamedDict(
        {
            'attrs': {
                'integration_id': {
                    'type': 'String',
                    'help': 'Tendrl managed/generated cluster id for the sds'
                    'being managed by Tendrl'},
                'cluster_name': {
                    'type': 'String',
                    'help': 'Name of the cluster'},
                'node_id': {
                    'type': 'String',
                    'help': 'Tendrl ID for the managed node'},
                'cluster_id': {
                    'type': 'String',
                    'help': 'UUID of the cluster'},
                'sds_version': {
                    'type': 'String',
                    'help': "Version of the Tendrl managed sds, eg: '3.2.1'"},
                'sds_name': {
                    'type': 'String',
                    'help': "Name of the Tendrl managed sds, eg: 'gluster'"}},
            'help': 'Tendrl context',
            'obj_list': '',
            'enabled': True,
            'obj_value': 'nodes/$NodeContext.node_id/TendrlContext',
            'flows': {},
            'atoms': {}})
    ret.flows["ImportCluster"] = {
        'help': 'Tendrl context',
        'enabled': True,
        'type': 'test_type',
        'flows': {},
        'atoms': {},
        'inputs': 'test_input',
        'uuid': 'test_uuid'}
    return ret



@mock.patch('tendrl.commons.event.Event.__init__',
            mock.Mock(return_value=None))
@mock.patch('tendrl.gluster_integration.sds_sync.GlusterIntegrationSdsSyncStateThread.run',
            mock.Mock(return_value=None))
@mock.patch('tendrl.commons.event.Event.__init__',
            mock.Mock(return_value=None))
@mock.patch(
    'tendrl.gluster_integration.objects.volume.Volume.save',
    mock.Mock(return_value=None)
)
@mock.patch(
    'tendrl.gluster_integration.objects.volume.Volume.load_all',
    mock.Mock(return_value=None)
)
@mock.patch(
    'tendrl.commons.objects.BaseObject.__init__',
    mock.Mock(return_value=None)
)
@mock.patch(
    'tendrl.gluster_integration.objects.utilization.Utilization.save',
    mock.Mock(return_value=None)
)
def test_run():
    setattr(NS, "publisher_id", "gluster-integration")
    setattr(NS, "gluster", maps.NamedDict())
    NS.gluster["objects"] = maps.NamedDict()

    # init()
    # param = maps.NamedDict()
    # param['integration_id'] = 'int-id'
    # param['enable_volume_profiling'] = 'yes'
    # cluster = NS.tendrl.objects.Cluster(**param)
    # print cluster
    # print cluster.enable_volume_profiling


    test_sync_thread = GlusterIntegrationSdsSyncStateThread()
    obj = importlib.import_module(
        'tendrl.gluster_integration.objects.volume'
    )
    # for obj_cls in inspect.getmembers(obj, inspect.isclass):
    #     NS.gluster.objects["Utilization"] = obj_cls[1]
    volume = Volume(
        vol_id='vol-id',
        vol_type='Replicate',
        name='vol1',
        status='Started',
        state='up',
        brick_count=3,
        replica_count=3,
        subvol_count=1,
        profiling_enabled='True'
    )
    assert volume.profiling_enabled == 'True'
