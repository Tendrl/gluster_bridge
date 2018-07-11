#!/usr/bin/python

import argparse

from tendrl.commons.utils import log_utils as logger
from tendrl.gluster_integration import gfapi


BYTES_IN_KB = 1024


def computeVolumeStats(data):
    total = data.f_blocks * data.f_bsize
    free = data.f_bfree * data.f_bsize
    used = total - free
    return {'sizeTotal': float(total),
            'sizeFree': float(free),
            'sizeUsed': float(used)}


def showVolumeUtilization(vname):
    try:
        data = gfapi.getVolumeStatvfs(vname)
    except gfapi.GlusterLibgfapiException as ex:
        logger.log(
            "critical",
            NS.publisher_id,
            {"message": "Failed to get the "
             "Volume Utilization Data. err: %s" % ex}
        )
        return
    volumeCapacity = computeVolumeStats(data)
    # total size in KB
    total_size = volumeCapacity['sizeTotal'] / BYTES_IN_KB
    # Available free size in KB
    free_size = volumeCapacity['sizeFree'] / BYTES_IN_KB
    # used size in KB
    used_size = volumeCapacity['sizeUsed'] / BYTES_IN_KB
    vol_utilization = (used_size / total_size) * 100
    used_inode = data.f_files - data.f_ffree
    total_inode = data.f_files
    pcnt_inode_used = (float(used_inode) / total_inode) * 100
    return (
        {
            'total': total_size,
            'free': free_size,
            'used': used_size,
            'pcnt_used': vol_utilization,
            'total_inode': total_inode,
            'used_inode': used_inode,
            'pcnt_inode_used': pcnt_inode_used
        }
    )


def parse_input():

    parser = argparse.ArgumentParser(
        usage='%(prog)s [-h] <volume>')
    parser.add_argument("volume",
                        help="Name of the volume to get the Utilization")
    args = parser.parse_args()
    return args
