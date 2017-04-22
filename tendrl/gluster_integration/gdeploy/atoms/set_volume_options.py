from gdeploy_features import *
from gdeploy_utils import cook_gdeploy_config
from gdeploy_utils import invoke_gdeploy


def set_volume_options(volume_name, hostname, options):
    """
    options should be of following form
    [
      {"option2": "Value2"},
      {"option3": "Value3"},
      {"option1": "Value1"},
    ]
    """
    recipe = []
    option_key_list = []
    option_value_list = []
    for option in options:
        option_key_list.append(option.keys()[0])
        option_value_list.append(option.values()[0])
    host_vol = hostname + ":" + volume_name
    recipe.append(
        get_volume(
            host_vol,
            "set",
            option_keys=option_key_list,
            option_values=option_value_list
        )
    )

    print recipe

    config_str = cook_gdeploy_config(recipe)

    print config_str
    out , err, rc = invoke_gdeploy(config_str)
    print out, err, rc

volume_name="tendrl_vol1"
hostname="10.70.42.40"
"""
options = [
    {"key1": "value1"},
    {"key2": "value2"},
    {"key3": "value3"},
    {"key4": "value4"},
]
"""
options = [
    {"features.ctr_lookupheal_link_timeout": "302"},
    {"features.ctr_lookupheal_inode_timeout": "301"},
]
set_volume_options(volume_name, hostname, options)
