from tendrl.gluster_bridge.persistence import servers


class Test_Peer(object):
    def test_peer(self):
        self.peer = servers.Peer()
        self.peer.__name__ = 'clusters/gluster/peers/%s'
        assert self.peer.render() == [
            {
                'name': 'hostname',
                'key': '/clusters/gluster/peers/None/hostname',
                'dir': False,
                'value': None
            },
            {
                'name': 'peer_uuid',
                'key': '/clusters/gluster/peers/None/peer_uuid',
                'dir': False, 'value': None
            },
            {
                'name': 'state',
                'key': '/clusters/gluster/peers/None/state',
                'dir': False, 'value': None
            },
            {
                'name': 'updated',
                'key': '/clusters/gluster/peers/None/updated',
                'dir': False,
                'value': None
            }]


class Test_Volume(object):
    def test_volume(self):
        self.volume = servers.Volume()
        self.volume.__name__ = 'clusters/gluster/volumes/%s'
        assert self.volume.render() == [
            {
                'dir': False,
                'name': 'brick_count',
                'key': '/clusters/gluster/volumes/None/brick_count',
                'value': None
            },
            {
                'dir': False,
                'name': 'name',
                'key': '/clusters/gluster/volumes/None/name',
                'value': None
            },
            {
                'dir': False,
                'name': 'status',
                'key': '/clusters/gluster/volumes/None/status',
                'value': None},
            {
                'dir': False,
                'name': 'vol_id',
                'key': '/clusters/gluster/volumes/None/vol_id',
                'value': None
            },
            {
                'dir': False,
                'name': 'vol_type',
                'key': '/clusters/gluster/volumes/None/vol_type',
                'value': None
            }]


class Test_Brick(object):
    def test_brick(self):
        self.brick = servers.Brick()
        self.brick.path = "/test/pytest"
        self.brick.__name__ = 'clusters/gluster/bricks/%s'
        assert self.brick.render() == [
            {
                'name': 'fs_type',
                'value': None,
                'dir': False,
                'key': '/clusters/gluster/bricks/_test_pytest/fs_type'
            },
            {
                'name': 'hostname',
                'value': None,
                'dir': False,
                'key': '/clusters/gluster/bricks/_test_pytest/hostname'
            },
            {
                'name': 'mount_opts',
                'value': None,
                'dir': False,
                'key': '/clusters/gluster/bricks/_test_pytest/mount_opts'
            },
            {
                'name': 'path',
                'value': '/test/pytest',
                'dir': False,
                'key': '/clusters/gluster/bricks/_test_pytest/path'
            },
            {
                'name': 'port',
                'value': None,
                'dir': False,
                'key': '/clusters/gluster/bricks/_test_pytest/port'
            },
            {
                'name': 'status',
                'value': None,
                'dir': False,
                'key': '/clusters/gluster/bricks/_test_pytest/status'
            },
            {
                'name': 'vol_id',
                'value': None,
                'dir': False,
                'key': '/clusters/gluster/bricks/_test_pytest/vol_id'
            }]
