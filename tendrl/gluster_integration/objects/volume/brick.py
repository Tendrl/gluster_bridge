import etcd

from tendrl.gluster_integration.config import TendrlConfig


class Brick(object):
    def __init__(self, cluster_id, vol_id, name):
        self.name = name
        self.cluster_id = cluster_id
        self.vol_id = vol_id
        self.config = TendrlConfig()
        etcd_kwargs = {
            'host': self.config.get("common", "etcd_connection"),
            'port': int(self.config.get("common", "etcd_port"))
        }
        self.client = etcd.Client(**etcd_kwargs)

    def refresh(self):
        path = "/clusters/%s/Volumes/%s/Bricks/%s" %\
            (self.cluster_id, self.vol_id, self.name)
        brick = self.client.read(path)
        for el in brick.children:
            key = el.key.split('/')[-1]
            if key == "path":
                self.path = el.value
            elif key == "port":
                self.port = el.value
            elif key == "status":
                self.status = el.value
            elif key == "fs_type":
                self.fs_type = el.value
            elif key == "hostname":
                self.hostname = el.value
            elif key == "mount_opts":
                self.mount_opts = el.value
