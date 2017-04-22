from gdeploy_features import *
from gdeploy_utils import cook_gdeploy_config
from gdeploy_utils import invoke_gdeploy


def volume_quota_config(volume_name, hostname, action, dir_details={}):
    """
    dir_details should be of following form
    [
      {"dir2": "size2"},
      {"dir1": "size1"},
      {"dir3": "size3"},
    ]
    """
    recipe = []
    host_vol = hostname + ":" + volume_name
    if dir_details:
        dir_list = []
        size_list = []
        for directory in dir_details:
            dir_list.append(directory.keys()[0])
            size_list.append(directory.values()[0])
        recipe.append(
            get_quota(
                host_vol,
                action,
                path=dir_list,
                size=size_list
            )
        )
    else:
        recipe.append(
            get_quota(
                host_vol,
                action
            )
        )

    print recipe

    config_str = cook_gdeploy_config(recipe)

    print config_str
    out , err, rc = invoke_gdeploy(config_str)
    print out, err, rc

volume_name="snap"
hostname="10.70.42.40"
"""
dir_details = [
    {"dir1": "size1"},
    {"dir2": "size2"},
    {"dir3": "size3"},
    {"dir4": "size4"},
]
"""
dir_details = [
    {"DIR1": "5MB"},
    {"DIR2": "5MB"},
]

action="limit-usage"
#action="enable"
volume_quota_config(volume_name, hostname,action,dir_details)
#volume_quota_config(volume_name, hostname, action)
