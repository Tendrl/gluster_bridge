import os

from tendrl.commons.utils import cmd_utils
from tendrl.commons.utils import log_utils as logger


def _get_mount_point(path):
    mount = os.path.realpath(path)
    while not os.path.ismount(mount):
        mount = os.path.dirname(mount)
    return mount


def _parse_proc_mounts(filter=True):
    mount_points = {}
    with open('/proc/mounts', 'r') as f:
        for line in f:
            if line.startswith("/") or not filter:
                mount = {}
                tokens = line.split()
                mount['device'] = tokens[0]
                mount['fsType'] = tokens[2]
                mount['mountOptions'] = tokens[3]
                mount_points[tokens[1]] = mount
    return mount_points


def _get_stats(mount_point):
    data = os.statvfs(mount_point)
    total = (data.f_blocks * data.f_bsize)
    free = (data.f_bfree * data.f_bsize)
    used_percent = 100 - (100.0 * free / total)
    total_inode = data.f_files
    free_inode = data.f_ffree
    used_percent_inode = 100 - (100.0 * free_inode / total_inode)
    used = total - free
    used_inode = total_inode - free_inode
    return {'total': total,
            'free': free,
            'used_percent': used_percent,
            'total_inode': total_inode,
            'free_inode': free_inode,
            'used_inode': used_inode,
            'used': used,
            'used_percent_inode': used_percent_inode}


def get_lvs():
    _lvm_cmd = ("lvm vgs --unquoted --noheading --nameprefixes "
                "--separator $ --nosuffix --units m -o lv_uuid,"
                "lv_name,data_percent,pool_lv,lv_attr,lv_size,"
                "lv_path,lv_metadata_size,metadata_percent,vg_name")
    cmd = cmd_utils.Command(_lvm_cmd, True)
    out, err, rc = cmd.run()
    if rc != 0:
        logger.log(
            "debug",
            NS.publisher_id,
            {"message": str(err)}
        )
        return None
    out = out.split('\n')
    lst = map(lambda x: dict(x),
              map(lambda x: [e.split('=') for e in x],
                  map(lambda x: x.strip().split('$'), out)))

    d = {}
    for i in lst:
        if i['LVM2_LV_ATTR'][0] == 't':
            k = "%s/%s" % (i['LVM2_VG_NAME'], i['LVM2_LV_NAME'])
        else:
            k = os.path.realpath(i['LVM2_LV_PATH'])
        d.update({k: i})
    return d


def get_mount_stats(mount_path, lvs):
    def _get_mounts(mount_path=[]):
        mount_list = map(_get_mount_point, mount_path)
        mount_points = _parse_proc_mounts()
        outList = set(mount_points).intersection(set(mount_list))
        # list comprehension to build dictionary does not work in python 2.6.6
        mounts = {}
        for k in outList:
            mounts[k] = mount_points[k]
        return mounts

    def _get_thin_pool_stat(device):
        out = {'thinpool_size': None,
               'thinpool_used_percent': None,
               'metadata_size': None,
               'metadata_used_percent': None,
               'thinpool_free': None,
               'metadata_free': None,
               'thinpool_used': None,
               'metadata_used': None}

        if lvs and device in lvs and \
           lvs[device]['LVM2_LV_ATTR'][0] == 'V':
            thinpool = "%s/%s" % (lvs[device]['LVM2_VG_NAME'],
                                  lvs[device]['LVM2_POOL_LV'])
            out['thinpool_size'] = float(
                lvs[thinpool]['LVM2_LV_SIZE']) / 1024
            out['thinpool_used_percent'] = float(
                lvs[thinpool]['LVM2_DATA_PERCENT'])
            out['metadata_size'] = float(
                lvs[thinpool]['LVM2_LV_METADATA_SIZE']) / 1024
            out['metadata_used_percent'] = float(
                lvs[thinpool]['LVM2_METADATA_PERCENT'])
            out['thinpool_free'] = out['thinpool_size'] * (
                1 - out['thinpool_used_percent'] / 100.0)
            out['thinpool_used'] = out['thinpool_size'] - out['thinpool_free']
            out['metadata_free'] = out['metadata_size'] * (
                1 - out['metadata_used_percent'] / 100.0)
            out['metadata_used'] = out['metadata_size'] - out['metadata_free']
        return out

    mount_points = _get_mounts(mount_path)
    mount_detail = {}
    for mount, info in mount_points.iteritems():
        mount_detail[mount] = _get_stats(mount)
        mount_detail[mount].update(
            _get_thin_pool_stat(os.path.realpath(info['device']))
        )
        mount_detail[mount].update({'mount_point': mount})
    return mount_detail


def brick_utilization(path, lvs):
    """{

         'used_percent': 0.6338674168297445,

         'used': 0.0316314697265625,

         'free_inode': 2621390,

         'used_inode': 50,

         'free': 4.9586029052734375,

         'total_inode': 2621440,

         'mount_point': u'/bricks/brick2',

         'metadata_used_percent': None,

         'total': 4.990234375,

         'thinpool_free': None,

         'metadata_used': None,

         'thinpool_used_percent': None,

         'used_percent_inode': 0.0019073486328125,

         'thinpool_used': None,

         'metadata_size': None,

         'metadata_free': None,

         'thinpool_size': None

    }
    """
    # Below logic will find mount_path from path
    mount_path = [path.split(":")[1]]
    return get_mount_stats(mount_path, lvs).values()[0]
