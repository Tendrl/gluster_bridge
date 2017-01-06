import etcd

from tendrl.gluster_integration.config import TendrlConfig
from tendrl.gluster_integration.objects.volume import brick


class Volume(object):
    def __init__(self, volname, vol_id, cluster_id):
        self.name = volname
        self.vol_id = vol_id
        self.cluster_id = cluster_id
        self.vol_type = ""
        self.bricks = None
        self.brick_count = 0
        self.deleted = False
        self.status = ""

        self.config = TendrlConfig()
        etcd_kwargs = {
            'host': self.config.get("common", "etcd_connection"),
            'port': int(self.config.get("common", "etcd_port"))
        }
        self.client = etcd.Client(**etcd_kwargs)

    def __populate_brick_details__(self):
        arr = []
        path = "/clusters/%s/Volumes/%s/Bricks" %\
            (self.cluster_id, self.vol_id)
        bricks = self.client.read(path)
        for el in bricks.children:
            brickIns = brick.Brick(
                self.cluster_id,
                self.vol_id,
                el.key.split('/')[-1]
            )
            brickIns.refresh()
            arr.append(brickIns)
        self.bricks = arr

    def __populate_volume_details__(self, volume):
        if volume is not None:
            for el in volume.children:
                key = el.key.split('/')[-1]
                if key == "name":
                    self.name = el.value
                elif key == "vol_type":
                    self.vol_type = el.value
                elif key == "brick_count":
                    self.brick_count = el.value
                elif key == "deleted":
                    self.deleted = el.value
                elif key == "status":
                    self.status = el.value
            # In case of new volume creation vol_id is not passed
            # Also population of brick details is not required as
            # volume is still under creation and no validations would
            # be done.
            if self.vol_id:
                self.__populate_brick_details__()

    def refresh(self):
        if self.vol_id:
            path = "/clusters/%s/Volumes/%s" % (self.cluster_id, self.vol_id)
            try:
                self.__populate_volume_details__(self.client.read(path))
            except etcd.EtcdKeyNotFound:
                # In case of volume deletion EtcdKeyNotFound would be raised
                # and its ok to pass for that case
                pass
        elif self.name:
            path = "/clusters/%s/Volumes" % self.cluster_id
            volumes = self.client.read(path)
            for volume in volumes.children:
                volumeIns = self.client.read(volume.key)
                for el in volumeIns.children:
                    if el.key.split('/')[-1] == "name" and \
                        el.value == self.name:
                        self.__populate_volume_details__(volumeIns)
