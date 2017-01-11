import logging
import os
import os.path
import subprocess

LOG = logging.getLogger(__name__)
TENDRL_CONTEXT = "/etc/tendrl/gluster-integration/tendrl_context"


def get_tendrl_context():
    # check if valid uuid is already present in tendrl_context
    # if not present generate one and update the file
    if os.path.isfile(TENDRL_CONTEXT):
        with open(TENDRL_CONTEXT) as f:
            cluster_id = f.read()
            LOG.info("Tendrl_context.cluster_id=%s found!" % cluster_id)
            return cluster_id


def set_tendrl_context(cluster_id):
    with open(TENDRL_CONTEXT, 'wb+') as f:
        f.write(cluster_id)
        LOG.info("Tendrl_context.cluster_id==%s created!" % cluster_id)


def get_sds_version():
    res = subprocess.check_output(['gluster', '--version'])
    return res.split()[1]
