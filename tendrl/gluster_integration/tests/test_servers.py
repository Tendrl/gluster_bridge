from tendrl.gluster_integration.persistence import servers


class Test_Peer(object):
    def test_peer(self):
        self.peer = servers.Peer()
        self.peer.cluster_id = '12345678-1234-5678-1234-567812345678'
        self.peer.peer_uuid = '12345678-1234-5678-1234-567812345678'
        assert self.peer.render() == [
            {
                'name': 'cluster_id',
                'key': '/clusters/12345678-1234-5678-1234-567812345678/'
                       'Peers/12345678-1234-5678-1234-567812345678/cluster_id',
                'dir': False,
                'value': '12345678-1234-5678-1234-567812345678'
            },
            {
                'name': 'hostname',
                'key': '/clusters/12345678-1234-5678-1234-567812345678/Peers/'
                       '12345678-1234-5678-1234-567812345678/hostname',
                'dir': False,
                'value': None
            },
            {
                'name': 'peer_uuid',
                'key': '/clusters/12345678-1234-5678-1234-567812345678/Peers/'
                       '12345678-1234-5678-1234-567812345678/peer_uuid',
                'dir': False,
                'value': '12345678-1234-5678-1234-567812345678'
            },
            {
                'name': 'state',
                'key': '/clusters/12345678-1234-5678-1234-567812345678/Peers/'
                       '12345678-1234-5678-1234-567812345678/state',
                'dir': False,
                'value': None
            },
            {
                'name': 'updated',
                'key': '/clusters/12345678-1234-5678-1234-567812345678/Peers/'
                       '12345678-1234-5678-1234-567812345678/updated',
                'dir': False,
                'value': None
            }]


class Test_Volume(object):
    def test_volume(self):
        self.volume = servers.Volume()
        self.volume.cluster_id = '12345678-1234-5678-1234-567812345678'
        self.volume.vol_id = '12345678-1234-5678-1234-567812345678'
        assert self.volume.render() == [
            {
                'dir': False,
                'name': 'arbiter_count',
                'key': '/clusters/12345678-1234-5678-1234-567812345678/'
                       'Volumes/12345678-1234-5678-1234-567812345678/'
                       'arbiter_count',
                'value': None
            },
            {
                'dir': False,
                'name': 'brick_count',
                'key': '/clusters/12345678-1234-5678-1234-567812345678/'
                       'Volumes/12345678-1234-5678-1234-567812345678/'
                       'brick_count',
                'value': None
            },
            {
                'dir': False,
                'name': 'cluster_id',
                'key': '/clusters/12345678-1234-5678-1234-567812345678/'
                       'Volumes/12345678-1234-5678-1234-567812345678/'
                       'cluster_id',
                'value': '12345678-1234-5678-1234-567812345678'
            },
            {
                'dir': False,
                'name': 'deleted',
                'key': '/clusters/12345678-1234-5678-1234-567812345678/'
                       'Volumes/12345678-1234-5678-1234-567812345678/'
                       'deleted',
                'value': None
            },
            {
                'dir': False,
                'name': 'disperse_count',
                'key': '/clusters/12345678-1234-5678-1234-567812345678/'
                       'Volumes/12345678-1234-5678-1234-567812345678/'
                       'disperse_count',
                'value': None
            },
            {
                'dir': False,
                'name': 'name',
                'key': '/clusters/12345678-1234-5678-1234-567812345678/'
                       'Volumes/12345678-1234-5678-1234-567812345678/'
                       'name',
                'value': None
            },
            {
                'dir': False,
                'name': 'quorum_status',
                'key': '/clusters/12345678-1234-5678-1234-567812345678/'
                       'Volumes/12345678-1234-5678-1234-567812345678/'
                       'quorum_status',
                'value': None
            },
            {
                'dir': False,
                'name': 'rebal_data',
                'key': '/clusters/12345678-1234-5678-1234-567812345678/'
                       'Volumes/12345678-1234-5678-1234-567812345678/'
                       'rebal_data',
                'value': None
            },
            {
                'dir': False,
                'name': 'rebal_failures',
                'key': '/clusters/12345678-1234-5678-1234-567812345678/'
                       'Volumes/12345678-1234-5678-1234-567812345678/'
                       'rebal_failures',
                'value': None
            },
            {
                'dir': False,
                'name': 'rebal_files',
                'key': '/clusters/12345678-1234-5678-1234-567812345678/'
                       'Volumes/12345678-1234-5678-1234-567812345678/'
                       'rebal_files',
                'value': None
            },
            {
                'dir': False,
                'name': 'rebal_id',
                'key': '/clusters/12345678-1234-5678-1234-567812345678/'
                       'Volumes/12345678-1234-5678-1234-567812345678/'
                       'rebal_id',
                'value': None
            },
            {
                'dir': False,
                'name': 'rebal_lookedup',
                'key': '/clusters/12345678-1234-5678-1234-567812345678/'
                       'Volumes/12345678-1234-5678-1234-567812345678/'
                       'rebal_lookedup',
                'value': None
            },
            {
                'dir': False,
                'name': 'rebal_skipped',
                'key': '/clusters/12345678-1234-5678-1234-567812345678/'
                       'Volumes/12345678-1234-5678-1234-567812345678/'
                       'rebal_skipped',
                'value': None
            },
            {
                'dir': False,
                'name': 'rebal_status',
                'key': '/clusters/12345678-1234-5678-1234-567812345678/'
                       'Volumes/12345678-1234-5678-1234-567812345678/'
                       'rebal_status',
                'value': None
            },
            {
                'dir': False,
                'name': 'redundancy_count',
                'key': '/clusters/12345678-1234-5678-1234-567812345678/'
                       'Volumes/12345678-1234-5678-1234-567812345678/'
                       'redundancy_count',
                'value': None
            },
            {
                'dir': False,
                'name': 'replica_count',
                'key': '/clusters/12345678-1234-5678-1234-567812345678/'
                       'Volumes/12345678-1234-5678-1234-567812345678/'
                       'replica_count',
                'value': None
            },
            {
                'dir': False,
                'name': 'snap_count',
                'key': '/clusters/12345678-1234-5678-1234-567812345678/'
                       'Volumes/12345678-1234-5678-1234-567812345678/'
                       'snap_count',
                'value': None
            },
            {
                'dir': False,
                'name': 'snapd_inited',
                'key': '/clusters/12345678-1234-5678-1234-567812345678/'
                       'Volumes/12345678-1234-5678-1234-567812345678/'
                       'snapd_inited',
                'value': None
            },
            {
                'dir': False,
                'name': 'snapd_status',
                'key': '/clusters/12345678-1234-5678-1234-567812345678/'
                       'Volumes/12345678-1234-5678-1234-567812345678/'
                       'snapd_status',
                'value': None
            },
            {
                'dir': False,
                'name': 'status',
                'key': '/clusters/12345678-1234-5678-1234-567812345678/'
                       'Volumes/12345678-1234-5678-1234-567812345678/'
                       'status',
                'value': None
            },
            {
                'dir': False,
                'name': 'stripe_count',
                'key': '/clusters/12345678-1234-5678-1234-567812345678/'
                       'Volumes/12345678-1234-5678-1234-567812345678/'
                       'stripe_count',
                'value': None
            },
            {
                'dir': False,
                'name': 'subvol_count',
                'key': '/clusters/12345678-1234-5678-1234-567812345678/'
                       'Volumes/12345678-1234-5678-1234-567812345678/'
                       'subvol_count',
                'value': None
            },
            {
                'dir': False,
                'name': 'transport_type',
                'key': '/clusters/12345678-1234-5678-1234-567812345678/'
                       'Volumes/12345678-1234-5678-1234-567812345678/'
                       'transport_type',
                'value': None
            },
            {
                'dir': False,
                'name': 'vol_id',
                'key': '/clusters/12345678-1234-5678-1234-567812345678/'
                       'Volumes/12345678-1234-5678-1234-567812345678/'
                       'vol_id',
                'value': '12345678-1234-5678-1234-567812345678'
            },
            {
                'dir': False,
                'name': 'vol_type',
                'key': '/clusters/12345678-1234-5678-1234-567812345678/'
                       'Volumes/12345678-1234-5678-1234-567812345678/'
                       'vol_type',
                'value': None
            }
        ]


class Test_Brick(object):
    def test_brick(self):
        self.brick = servers.Brick()
        self.brick.cluster_id = '12345678-1234-5678-1234-567812345678'
        self.brick.vol_id = '12345678-1234-5678-1234-567812345678'
        self.brick.path = "/test/pytest"
        assert self.brick.render() == [
            {
                'name': 'cluster_id',
                'key': '/clusters/12345678-1234-5678-1234-567812345678/'
                       'Volumes/12345678-1234-5678-1234-567812345678/Bricks/'
                       '_test_pytest/cluster_id',
                'dir': False,
                'value': '12345678-1234-5678-1234-567812345678'
            },
            {
                'name': 'fs_type',
                'value': None,
                'dir': False,
                'key': '/clusters/12345678-1234-5678-1234-567812345678/'
                       'Volumes/12345678-1234-5678-1234-567812345678/Bricks/'
                       '_test_pytest/fs_type'
            },
            {
                'name': 'hostname',
                'value': None,
                'dir': False,
                'key': '/clusters/12345678-1234-5678-1234-567812345678/'
                       'Volumes/12345678-1234-5678-1234-567812345678/Bricks/'
                       '_test_pytest/hostname'
            },
            {
                'name': 'mount_opts',
                'value': None,
                'dir': False,
                'key': '/clusters/12345678-1234-5678-1234-567812345678/'
                       'Volumes/12345678-1234-5678-1234-567812345678/Bricks/'
                       '_test_pytest/mount_opts'
            },
            {
                'name': 'path',
                'value': '/test/pytest',
                'dir': False,
                'key': '/clusters/12345678-1234-5678-1234-567812345678/'
                       'Volumes/12345678-1234-5678-1234-567812345678/Bricks/'
                       '_test_pytest/path'
            },
            {
                'name': 'port',
                'value': None,
                'dir': False,
                'key': '/clusters/12345678-1234-5678-1234-567812345678/'
                       'Volumes/12345678-1234-5678-1234-567812345678/Bricks/'
                       '_test_pytest/port'
            },
            {
                'name': 'status',
                'value': None,
                'dir': False,
                'key': '/clusters/12345678-1234-5678-1234-567812345678/'
                       'Volumes/12345678-1234-5678-1234-567812345678/Bricks/'
                       '_test_pytest/status'
            },
            {
                'name': 'vol_id',
                'value': '12345678-1234-5678-1234-567812345678',
                'dir': False,
                'key': '/clusters/12345678-1234-5678-1234-567812345678/'
                       'Volumes/12345678-1234-5678-1234-567812345678/Bricks/'
                       '_test_pytest/vol_id'
            }]
