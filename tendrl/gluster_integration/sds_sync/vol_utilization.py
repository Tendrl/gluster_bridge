#!/usr/bin/python

import argparse
import json
import sys

from tendrl.gluster_integration import gfapi


BYTES_IN_KB = 1024


def computeVolumeStats(data):
    total = data.f_blocks * data.f_bsize
    free = data.f_bfree * data.f_bsize
    used = total - free
    return {'sizeTotal': float(total),
            'sizeFree': float(free),
            'sizeUsed': float(used)}


def showVolumeUtilization(vnames):
    volumes = vnames.split(',')
    ret_dict = {}
    for volume in volumes:
        if volume != "":
            try:
                data = gfapi.getVolumeStatvfs(volume)
            except gfapi.GlusterLibgfapiException as e:
                if 'error' not in ret_dict.keys():
                    ret_dict['error'] = {}
                ret_dict['error'][volume] = \
                    {'message': "CRITICAL: Failed to "
                                "get the Volume Utilization Data."
                                "Error: %s" % e}
                continue
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
            if 'success' not in ret_dict.keys():
                ret_dict['success'] = {}
            ret_dict['success'][volume] = {
                'total': total_size,
                'free': free_size,
                'used': used_size,
                'pcnt_used': vol_utilization,
                'total_inode': total_inode,
                'used_inode': used_inode,
                'pcnt_inode_used': pcnt_inode_used
            }

    print(json.dumps(ret_dict))


def parse_input():

    parser = argparse.ArgumentParser(
        usage='%(prog)s [-h] <volumes>')
    parser.add_argument("volumes",
                        help="Names of the volumes to get the Utilization")
    args = parser.parse_args()
    return args


def main():
    args = parse_input()
    showVolumeUtilization(args.volumes)
    sys.exit(0)


if __name__ == '__main__':
    main()
