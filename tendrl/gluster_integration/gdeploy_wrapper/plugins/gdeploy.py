from tendrl.commons.utils import log_utils as logger
try:
    from python_gdeploy.actions import create_gluster_volume
    from python_gdeploy.actions import delete_volume
    from python_gdeploy.actions import expand_gluster_volume
    from python_gdeploy.actions import gluster_brick_provision
    from python_gdeploy.actions import rebalance_volume
    from python_gdeploy.actions import shrink_gluster_volume
    from python_gdeploy.actions import start_volume
    from python_gdeploy.actions import stop_volume
except ImportError:
    logger.log(
        "info",
        NS.publisher_id,
        {"message": "python-gdeploy is not installed in this node"},
        integration_id=NS.tendrl_context.integration_id
    )

from tendrl.gluster_integration.gdeploy_wrapper.provisioner_base import\
    ProvisionerBasePlugin


class GdeployPlugin(ProvisionerBasePlugin):
    def __init__(self):
        ProvisionerBasePlugin.__init__(self)

    def create_volume(self, volume_name, brick_details, transport=None,
                      replica_count=None, disperse_count=None,
                      redundancy_count=None, tuned_profile=None, force=False):
        args = {}
        if transport:
            args.update({"transport": transport})
        if replica_count:
            args.update({"replica_count": replica_count})
        if disperse_count:
            args.update({"disperse_count": disperse_count})
        if redundancy_count:
            args.update({"redundancy_count": redundancy_count})
        if tuned_profile:
            args.update({"tuned_profile": tuned_profile})
        if force:
            args.update({"force": force})

        out, err, rc = create_gluster_volume.create_volume(
            volume_name,
            brick_details,
            **args
        )
        if rc == 0:
            logger.log(
                "debug",
                NS.publisher_id,
                {"message": "gluster volume %s created successfully" %
                 volume_name},
                integration_id=NS.tendrl_context.integration_id
            )
        else:
            logger.log(
                "debug",
                NS.publisher_id,
                {"message": "gluster volume creation failed for %s."
                 " Details: %s" % (volume_name, out)},
                integration_id=NS.tendrl_context.integration_id
            )
            return False
        return True

    def delete_volume(self, volume_name, host=None, force=None,
                      format_bricks=None):
        args = {}
        if host:
            args.update({"host": host})
        if force:
            args.update({"force": force})

        out, err, rc = delete_volume.delete_volume(
            volume_name,
            **args
        )
        if rc == 0:
            logger.log(
                "info",
                NS.publisher_id,
                {"message": "gluster volume %s deleted successfully" %
                 volume_name},
                integration_id=NS.tendrl_context.integration_id
            )
        else:
            logger.log(
                "debug",
                NS.publisher_id,
                {"message": "Volume deletion failed for volume "
                 "%s. Details: %s" % (volume_name, out)},
                integration_id=NS.tendrl_context.integration_id
            )
            return False
        if format_bricks:
            pass
            # TODO(darshan) Call gdeploy action to clear brick
        return True

    def start_volume(self, volume_name, host=None, force=None):
        args = {}
        if host:
            args.update({"host": host})
        if force:
            args.update({"force": force})

        out, err, rc = start_volume.start_volume(
            volume_name,
            **args
        )
        if rc == 0:
            logger.log(
                "info",
                NS.publisher_id,
                {"message": "Volume %s started successfully" %
                 volume_name},
                integration_id=NS.tendrl_context.integration_id
            )
        else:
            logger.log(
                "debug",
                NS.publisher_id,
                {"message": "Volume start failed for volume "
                 "%s. Details: %s" % (volume_name, str(out))},
                integration_id=NS.tendrl_context.integration_id
            )
            return False
        return True

    def stop_volume(self, volume_name, host=None, force=None):
        args = {}
        if host:
            args.update({"host": host})
        if force:
            args.update({"force": force})

        out, err, rc = stop_volume.stop_volume(
            volume_name,
            **args
        )
        if rc == 0:
            logger.log(
                "info",
                NS.publisher_id,
                {"message": "Volume %s stopped successfully" %
                 volume_name},
                integration_id=NS.tendrl_context.integration_id
            )
        else:
            logger.log(
                "debug",
                NS.publisher_id,
                {"message": "Volume stop failed for volume "
                 "%s. Details: %s" % (volume_name, out)},
                integration_id=NS.tendrl_context.integration_id
            )
            return False
        return True

    def rebalance_volume(self, volume_name, action, host=None,
                         force=None, fix_layout=None):
        args = {}
        if host:
            args.update({"host": host})
        if force:
            args.update({"force": force})
        if fix_layout and action == "start":
            action = "fix-layout"

        out, err, rc = rebalance_volume.rebalance_volume(
            volume_name,
            action,
            **args
        )
        if rc == 0:
            logger.log(
                "info",
                NS.publisher_id,
                {"message": "Rebalance %s on volume %s performed"
                 "successfully" % (action, volume_name)},
                integration_id=NS.tendrl_context.integration_id
            )
        else:
            logger.log(
                "debug",
                NS.publisher_id,
                {"message": "Rebalance %s failed for volume "
                 "%s. Details: %s" % (action, volume_name, out)},
                integration_id=NS.tendrl_context.integration_id
            )
            return False
        return True

    def expand_volume(self, volume_name, brick_details,
                      replica_count=None, disperse_count=None,
                      force=False,
                      increase_replica_count=False):
        args = {}
        if replica_count:
            args.update({"replica_count": replica_count})
        if disperse_count:
            args.update({"disperse_count": disperse_count})
        if force:
            args.update({"force": force})
        if increase_replica_count:
            args.update({"increase_replica_count": increase_replica_count})

        out, err, rc = expand_gluster_volume.expand_volume(
            volume_name,
            brick_details,
            **args
        )
        if rc == 0:
            logger.log(
                "info",
                NS.publisher_id,
                {"message": "Volume %s expanded successfully" %
                 volume_name},
                integration_id=NS.tendrl_context.integration_id
            )
        else:
            logger.log(
                "debug",
                NS.publisher_id,
                {"message": "Volume expansion failed for volume "
                 "%s. Details: %s" % (volume_name, out)},
                integration_id=NS.tendrl_context.integration_id
            )
            return False
        return True

    def shrink_volume(self, volume_name, brick_details, action,
                      replica_count=None, disperse_count=None,
                      force=False,
                      decrease_replica_count=False):
        args = {}
        if replica_count:
            args.update({"replica_count": replica_count})
        if disperse_count:
            args.update({"disperse_count": disperse_count})
        if force:
            args.update({"force": force})
        if decrease_replica_count:
            args.update({"decrease_replica_count": decrease_replica_count})

        out, err, rc = shrink_gluster_volume.shrink_gluster_volume(
            volume_name,
            brick_details,
            action,
            **args
        )
        if rc == 0:
            logger.log(
                "info",
                NS.publisher_id,
                {"message": "Volume %s shrinked successfully" %
                 volume_name},
                integration_id=NS.tendrl_context.integration_id
            )
        else:
            logger.log(
                "info",
                NS.publisher_id,
                {"message": "Volume shrink failed for volume "
                 "%s. Details: %s" % (volume_name, out)},
                integration_id=NS.tendrl_context.integration_id
            )
            return False
        return True

    def gluster_provision_bricks(self, brick_dictionary, disk_type=None,
                                 disk_count=None, stripe_count=None):
        out, err, rc = gluster_brick_provision.provision_disks(
            brick_dictionary,
            disk_type,
            disk_count,
            stripe_count
        )
        if rc == 0 and err == "":
            logger.log(
                "info",
                NS.publisher_id,
                {"message": "Bricks Provisioned successfully"},
                integration_id=NS.tendrl_context.integration_id
            )
        else:
            logger.log(
                "info",
                NS.publisher_id,
                {"message": "Bricks Provisioning Failed. Error %s" % (
                    str(out))},
                integration_id=NS.tendrl_context.integration_id
            )
            return False
        return True
