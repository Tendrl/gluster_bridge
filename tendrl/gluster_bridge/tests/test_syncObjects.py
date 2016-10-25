from tendrl.gluster_bridge.persistence.sync_objects import SyncObject


class Test_SyncObject(object):

    def test_SyncObject(self):
        self.sync_object = SyncObject
        assert self.sync_object.data.name == "data"
        assert self.sync_object.updated.name == "updated"
