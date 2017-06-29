import etcd
import json


def format_brick_paths(brick_details):
    flattened_brick_list = []

    for sub_vol in brick_details:
        for brick in sub_vol:
            ip = brick.keys()[0]
            brick_path = brick.values()[0]
            try:
                # Find node id using ip
                node_id = NS._int.client.read("indexes/ip/%s" % ip).value
            except etcd.EtcdKeyNotFound:
                # Find node id using hostname
                nodes = NS._int.client.read("nodes/")
                for node in nodes.leaves:
                    hostname = NS._int.client.read(
                        "%s/NodeContext/fqdn" % node.key).value
                    if hostname == ip:
                        node_id = node.key.split("/")[-1]
            key = "nodes/%s/NodeContext/fqdn" % node_id
            host = NS._int.client.read(key).value
            brick_path = host + ":" + brick_path
            flattened_brick_list.append((node_id, brick_path))
    return flattened_brick_list

def lock_bricks(brick_paths, lock_info):
    for entry in brick_paths:
        brick_lock_key = "clusters/%s/nodes/%s/GlusterBricks/all/%s/locked_by" % \
            (
                NS.tendrl_context.integration_id,
                entry[0],
                entry[1].replace("/","_")
            )
        NS._int.client.write(brick_lock_key, json.dumps(lock_info))

def unlock_bricks(brick_paths):
    for entry in brick_paths:
        try:
            brick_lock_key = "clusters/%s/nodes/%s/GlusterBricks/all/%s/locked_by" % \
                (
                    NS.tendrl_context.integration_id,
                    entry[0],
                    entry[1].replace("/","_")
                )
            NS._int.client.delete(brick_lock_key, recursive=True)
        except etcd.EtcdKeyNotFound:
            # Ignore as its just removal of lock
            pass
