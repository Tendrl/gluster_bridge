from mock import MagicMock
import sys

sys.modules['tendrl.gluster_integration.objects.volume.atoms.create'] = \
    MagicMock()
sys.modules['etcd'] = MagicMock()
from tendrl.gluster_integration.flows.create_volume import CreateVolume


class TestCreateVolume(object):

    def test_start(object):
        flow = CreateVolume(
            {
                "request_id": "49fa2adde8a6e98591f0f5cb4bc5f44d",
                "sds_type": "gluster",
                "flow": "CreateVolume",
                "object_type": "volume",
                "status": 'new',
                "message": 'Creating cluster',
                "parameters": {
                    "volname": 'Volume1',
                    "brickdetails": ["mntpath"]
                },
                "errors": {}
            })
        flow.start()
        flow.atom().start.assert_called_with(
            'Volume1',
            ['mntpath'],
            transport=None
        )
        assert flow.api_job['status'] == "finished"

        flow1 = CreateVolume(
            {
                "request_id": "49fa2adde8a6e98591f0f5cb4bc5f44d",
                "sds_type": "gluster",
                "flow": "CreateVolume",
                "object_type": "volume",
                "status": 'new',
                "message": 'Creating cluster',
                "parameters": {
                    "volname": 'Volume1',
                    "brickdetails": ["mntpath"],
                    "stripe_count": 3
                },
                "errors": {}
            })
        flow1.start()
        flow1.atom().start.assert_called_with(
            'Volume1',
            ['mntpath'],
            stripe_count=3,
            transport=None
        )
        assert flow1.api_job['status'] == "finished"

        flow2 = CreateVolume(
            {
                "request_id": "49fa2adde8a6e98591f0f5cb4bc5f44d",
                "sds_type": "gluster",
                "flow": "CreateVolume",
                "object_type": "volume",
                "status": 'new',
                "message": 'Creating cluster',
                "parameters": {
                    "volname": 'Volume1',
                    "brickdetails": ["mntpath"],
                    "replica_count": 3,
                    "arbiter_count": 1,
                },
                "errors": {}
            })
        flow2.start()
        flow2.atom().start.assert_called_with(
            'Volume1',
            ['mntpath'],
            replica_count=3,
            transport=None,
            arbiter_count=1
        )
        assert flow2.api_job['status'] == "finished"

        flow3 = CreateVolume(
            {
                "request_id": "49fa2adde8a6e98591f0f5cb4bc5f44d",
                "sds_type": "gluster",
                "flow": "CreateVolume",
                "object_type": "volume",
                "status": 'new',
                "message": 'Creating cluster',
                "parameters": {
                    "volname": 'Volume1',
                    "brickdetails": ["mntpath"],
                    "replica_count": 3
                },
                "errors": {}
            })
        flow3.start()
        flow3.atom().start.assert_called_with(
            'Volume1',
            ['mntpath'],
            replica_count=3,
            arbiter_count=None,
            transport=None
        )
        assert flow3.api_job['status'] == "finished"

        flow4 = CreateVolume(
            {
                "request_id": "49fa2adde8a6e98591f0f5cb4bc5f44d",
                "sds_type": "gluster",
                "flow": "CreateVolume",
                "object_type": "volume",
                "status": 'new',
                "message": 'Creating cluster',
                "parameters": {
                    "volname": 'Volume1',
                    "brickdetails": ["mntpath"],
                    "disperse_count": 3,
                    "disperse_data_count": 1,
                    "redundancy_count": 2
                },
                "errors": {}
            })
        flow4.start()
        flow4.atom().start.assert_called_with(
            'Volume1',
            ['mntpath'],
            disperse_count=3,
            disperse_data_count=1,
            redundancy_count=2,
            transport=None
        )
        assert flow4.api_job['status'] == "finished"

        flow5 = CreateVolume(
            {
                "request_id": "49fa2adde8a6e98591f0f5cb4bc5f44d",
                "sds_type": "gluster",
                "flow": "CreateVolume",
                "object_type": "volume",
                "status": 'new',
                "message": 'Creating cluster',
                "parameters": {
                    "volname": 'Volume1',
                    "brickdetails": ["mntpath"],
                    "disperse_count": 3,
                    "redundancy_count": 2
                },
                "errors": {}
            })
        flow5.start()
        flow5.atom().start.assert_called_with(
            'Volume1',
            ['mntpath'],
            disperse_count=3,
            disperse_data_count=None,
            redundancy_count=2,
            transport=None
        )
        assert flow5.api_job['status'] == "finished"

        flow6 = CreateVolume(
            {
                "request_id": "49fa2adde8a6e98591f0f5cb4bc5f44d",
                "sds_type": "gluster",
                "flow": "CreateVolume",
                "object_type": "volume",
                "status": 'new',
                "message": 'Creating cluster',
                "parameters": {
                    "volname": 'Volume1',
                    "brickdetails": ["mntpath"],
                    "disperse_count": 3,
                },
                "errors": {}
            })
        flow6.start()
        flow6.atom().start.assert_called_with(
            'Volume1',
            ['mntpath'],
            disperse_count=3,
            disperse_data_count=None,
            redundancy_count=None,
            transport=None
        )
        assert flow6.api_job['status'] == "finished"
