from gdeploy_features import *
from gdeploy_utils import cook_gdeploy_config
from gdeploy_utils import invoke_gdeploy


def volume_snapshot_config(volume_name, hostname, action, snapname=""):
    recipe = []
    host_vol = hostname + ":" + volume_name
    recipe.append(
        get_snapshot(
            host_vol,
            action,
            snapname
        )
    )

    print recipe

    config_str = cook_gdeploy_config(recipe)

    print config_str
    out , err, rc = invoke_gdeploy(config_str)
    print out, err, rc

volume_name="snap"
hostname="10.70.42.40"
action="delete"
snapname = "snap1"
volume_snapshot_config(volume_name, hostname, action, snapname)
