from gdeploy_features import *
from gdeploy_utils import cook_gdeploy_config
from gdeploy_utils import invoke_gdeploy


def provision_disks(disk_dictionary):
    """Structure of disk dictionary
    
    {"host_name_0": {
           "diks_name_0": {
                   "mount_path": <actual-mountpath>,
                   "brick_path": <actual-brick-path>
           },
           "diks_name_1": {
                   "mount_path": <actual-mountpath>,
                   "brick_path": <actual-brick-path>
           },
           "diks_name_2": {
                   "mount_path": <actual-mountpath>,
                   "brick_path": <actual-brick-path>
           }
     },
    "host_name_2": {
           "diks_name_0": {
                   "mount_path": <actual-mountpath>,
                   "brick_path": <actual-brick-path>
           },
           "diks_name_1": {
                   "mount_path": <actual-mountpath>,
                   "brick_path": <actual-brick-path>
           },
           "diks_name_2": {
                   "mount_path": <actual-mountpath>,
                   "brick_path": <actual-brick-path>
           },
     }
    }
    """
    recipe = []
    recipe.append(get_hosts(disk_dictionary.keys()))
    for host, disks in disk_dictionary.iteritems():
        device_list = []
        mount_point_list = []
        brick_path_list = []
        for disk, detail in disks.iteritems():
            device_list.append(disk)
            mount_point_list.append(detail["mount_path"])
            brick_path_list.append(detail["brick_path"])
        recipe.append(
            get_backend_setup(
                device_list,
                mount_points=mount_point_list,
                brick_dirs=brick_path_list,
                target_host=host
            )
        )

    print recipe

    config_str = cook_gdeploy_config(recipe)

    print config_str
    out , err, rc = invoke_gdeploy(config_str)
    print out, err, rc

"""
d = {
    "10.70.23.23" : {
        "vda": {
            "mount_path": "/mnt/tendrl_b1",
            "brick_path": "/mnt/tendrl_b1/b1",
        },
        "vdb": {
            "mount_path": "/mnt/tendrl_b2",
            "brick_path": "/mnt/tendrl_b2/b2",
        },
        "vdc": {
            "mount_path": "/mnt/tendrl_b3",
            "brick_path": "/mnt/tendrl_b3/b3",
        },
    },
    "10.70.24.456" : {
        "vda": {
            "mount_path": "/mnt/tendrl_b1",
            "brick_path": "/mnt/tendrl_b1/b1",
        },
        "vdb": {
            "mount_path": "/mnt/tendrl_b2",
            "brick_path": "/mnt/tendrl_b2/b2",
        },
        "vdc": {
            "mount_path": "/mnt/tendrl_b3",
            "brick_path": "/mnt/tendrl_b3/b3",
        },
    },
    "10.70.28.211" : {
        "vda": {
            "mount_path": "/mnt/tendrl_b1",
            "brick_path": "/mnt/tendrl_b1/b1",
        },
        "vdb": {
            "mount_path": "/mnt/tendrl_b2",
            "brick_path": "/mnt/tendrl_b2/b2",
        },
        "vdc": {
            "mount_path": "/mnt/tendrl_b3",
            "brick_path": "/mnt/tendrl_b3/b3",
        },
    }
}

"""
d = {
    "10.70.42.40" : {
        "vdb": {
            "mount_path": "/mnt/tendrl_b1",
            "brick_path": "/mnt/tendrl_b1/b1",
        },
    }
}
provision_disks(d)
