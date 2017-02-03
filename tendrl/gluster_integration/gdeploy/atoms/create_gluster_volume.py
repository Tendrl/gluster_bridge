from gdeploy_features import *
from gdeploy_utils import cook_gdeploy_config
from gdeploy_utils import invoke_gdeploy


def create_volume(volume_name, brick_details, transport=[],
                  replica_count="", force=False):
    """
    Brick details should be of following form
    [
      {"hostname2": ["brick1","brick2"]},
      {"hostname3": ["brick1","brick2"]},
      {"hostname1": ["brick1","brick2"]},
    ]
    """
    recipe = []
    brick_list = []
    host_list = []
    for host in brick_details:
        host_list.append(host.keys()[0])
        for brick in host.values()[0]:
            brick_list.append(host.keys()[0] + ":" + brick)
    recipe.append(get_hosts(host_list))

    recipe.append(
        get_volume(
            volume_name,
            "create",
            brick_dirs=brick_list,
            transport=transport,
            replica_count=replica_count,
            force=force
        )
    )

    print recipe

    config_str = cook_gdeploy_config(recipe)

    print config_str
    out , err, rc = invoke_gdeploy(config_str)
    print out, err, rc

volume_name="tendrl_vol1"
"""
brick_details = [
    {"hostname2": ["brick1","brick2"]},
    {"hostname3": ["brick1","brick2"]},
    {"hostname1": ["brick1","brick2"]},
]
"""
brick_details = [
    {"10.70.43.136": ["/mnt/tendrl_vol1_b1","/mnt/tendrl_vol1_b2"]},
    {"10.70.42.40": ["/mnt/tendrl_vol1_b3","/mnt/tendrl_vol1_b4"]},
]

transport = ["tcp"]
replicacount = 2
force=True
create_volume(volume_name,brick_details,transport,replicacount,force)
