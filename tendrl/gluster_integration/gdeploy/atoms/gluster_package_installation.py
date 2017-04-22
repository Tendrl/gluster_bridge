from tendrl.gluster_integration.gdeploy.gdeploy_features import *
from tendrl.gluster_integration.gdeploy.gdeploy_utils import cook_gdeploy_config
from tendrl.gluster_integration.gdeploy.gdeploy_utils import invoke_gdeploy

GLUSTERFS_PACKAGES = [
    "glusterfs",
    "glusterfs-server",
    "glusterfs-cli",
    "glusterfs-libs",
    "glusterfs-client-xlators",
    "glusterfs-api",
    "glusterfs-fuse"
]

GLUSTERFS_REPO = "https://buildlogs.centos.org/centos/7/storage/x86_64/gluster-3.9/"


def gluster_package_installation(host_list, glusterfs_packages=None, glusterfs_repo=None, gpgcheck=None):
    recipe = []

    recipe.append(get_hosts(host_list))
    
    recipe.append(
        get_yum(
            "install",
            glusterfs_packages if glusterfs_packages else GLUSTERFS_PACKAGES,
            glusterfs_repo if glusterfs_repo else GLUSTERFS_REPO,
            gpgcheck if gpgcheck else "no"
        )
    )

    config_str = cook_gdeploy_config(recipe)

    return invoke_gdeploy(config_str)
