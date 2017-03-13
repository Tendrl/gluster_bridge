# flake8: noqa
data="""---
namespace.tendrl.gluster_integration:
  flows:
    CreateVolume:
      atoms:
        - tendrl.gluster_integration.objects.Volume.atoms.create.Create
      help: "Create Volume with bricks"
      enabled: true
      inputs:
        mandatory:
          - Volume.volname
          - Volume.bricks
        optional:
          - Volume.stripe_count
          - Volume.replica_count
          - Volume.arbiter_count
          - Volume.disperse_count
          - Volume.disperse_data_count
          - Volume.redundancy_count
          - Volume.transport
          - Volume.force
      pre_run:
        - tendrl.gluster_integration.objects.Volume.atoms.named_volume_not_exists.NamedVolumeNotExists
      run: tendrl.gluster_integration.flows.create_volume.CreateVolume
      type: Create
      uuid: 1951e821-7aa9-4a91-8183-e73bc8275b8e
      version: 1
  objects:
    GlobalDetails:
      attrs:
        status:
          help: status of the cluster
          type: String
      enabled: true
      list: clusters/$TendrlContext.integration_id/GlobalDetails
      value: clusters/$TendrlContext.integration_id/GlobalDetails
      help: Clustr global details
    Utilization:
      attrs:
        raw_capacity:
          help: Raw capacity of cluster
          type: int
        usable_capacity:
          help: Usable capacity
          type: int
        used_capacity:
          help: Used capacity
          type: int
        pcnt_used:
          help: Percent usage
          type: int
      enabled: true
      list: clusters/$TendrlContext.integration_id/Utilization
      value: clusters/$TendrlContext.integration_id/Utilization
      help: Cluster utilization
    NodeContext:
      attrs:
        machine_id:
          help: "Unique /etc/machine-id"
          type: String
        fqdn:
          help: "FQDN of the Tendrl managed node"
          type: String
        node_id:
          help: "Tendrl ID for the managed node"
          type: String
        tags:
          help: "The tags associated with this node"
          type: String
        status:
          help: "Node status"
          type: String

      enabled: true
      list: clusters/$TendrlContext.integration_id/nodes/$NodeContext.node_id/NodeContext
      value: clusters/$TendrlContext.integration_id/nodes/$NodeContext.node_id/NodeContext
      help: Node Context

    Peer:
      enabled: true
      objects:
        hostname:
          help: "Gluster Peer hostname"
          type: String
        peer_uuid:
          help: "Gluster Peer uuid"
          type: String
        state:
          help: "Gluster Peer state"
          type: String
        update:
          help: "Last Peer update time"
          type: String
      value: clusters/$TendrlContext.integration_id/Peers/$Peer.peer_uuid
      list: clusters/$TendrlContext.integration_id/Peers
      help: Gluster cluster peers
    Config:
      attrs:
        data:
          help: Configuration data of Gluster Integration for this Tendrl deployment
          type: String
        etcd_connection:
          help: Host/IP of the etcd central store for this Tendrl deployment
          type: String
        etcd_port:
          help: Port of the etcd central store for this Tendrl deployment
          type: String
        file_path:
          default: /etc/tendrl/gluster-integration/gluster-integration.conf.yaml
          help: Path to the Gluster integration tendrl configuration
          type: String
      enabled: true
      value: _tendrl/config/gluster-integration
      list: _tendrl/config/gluster-integration
      help: gluster integration component configuration
    TendrlContext:
      attrs:
        integration_id:
          help: "Tendrl managed/generated cluster id for the sds being managed by Tendrl"
          type: String
        integration_name:
          help: "Tendrl managed/generated cluster name for the sds being managed by Tendrl"
          type: String
        sds_name:
          help: "Name of the SDS to which this cluster belongs"
          type: String
        sds_version:
          help: "Version of the SDS to which this cluster belongs"
          type: String
      enabled: true
      value: clusters/$TendrlContext.integration_id/TendrlContext/
      list: clusters/TendrlContext.integration_id/TendrlContext/
      help: gluster integration tendrl context
    SyncObject:
      attrs:
        cluster_id:
          help: "Tendrl managed/generated cluster id for the sds being managed by Tendrl"
          type: String
        data:
          help: raw data
          type: String
      enabled: true
      value: clusters/$TendrlContext.integration_id/raw_map/
      list: clusters/TendrlContext.integration_id/TendrlContext/raw_map/
      help: gluster cluster details
    VolumeOptions:
      attrs:
        cluster_id:
          help: "Tendrl managed/generated cluster id for the sds being managed by Tendrl"
          type: String
        vol_id:
          help: Volume id
          type: String
        options:
          help: options
          type: dict
      enabled: true
      value: clusters/$TendrlContext.integration_id/Volumes/$Volume.vol_id/options
      list: clusters/$TendrlContext.integration_id/Volumes/$Volume.vol_id/options
      help: gluster volume options
    Brick:
      attrs:
        vol_id:
          help: Volume id
          type: String
        path:
          help: Path of the brick
          type: String
        hostname:
          help: Name of the host
          type: String
        port:
          help: Brick port
          type: int
        status:
          help: Status of the brick
          type: String
        filesystem_type:
          help: File system type
          type: String
        mount_opts:
          help: Mount options
          type: String
      enabled: true
      value: clusters/$TendrlContext.integration_id/Volumes/$Volume.vol_id/Bricks/$Brick.path
      list: clusters/$TendrlContext.integration_id/Volumes/$Volume.vol_id/Bricks
      help: gluster volume bricks
    Volume:
      atoms:
        Create:
          enabled: true
          inputs:
            mandatory:
              - Volume.volname
              - Volume.bricks
            optional:
              - Volume.stripe_count
              - Volume.replica_count
              - Volume.arbiter_count
              - Volume.disperse_count
              - Volume.disperse_data_count
              - Volume.redundancy_count
              - Volume.transport
              - Volume.force
          name: create_volume
          run: tendrl.gluster_integration.objects.Volume.atoms.create.Create
          type: Create
          uuid: 242f6190-9b37-11e6-950d-a24fc0d9649c
        Delete:
          enabled: true
          inputs:
            mandatory:
              - Volume.volname
              - Volume.vol_id
          name: delete_volume
          run: tendrl.gluster_integration.objects.Volume.atoms.delete.Delete
          type: Delete
          uuid: 242f6190-9b37-11e6-950d-a24fc0d9650c
        Start:
          enabled: true
          inputs:
            mandatory:
              - Volume.volname
          name: start_volume
          run: tendrl.gluster_integration.objects.Volume.atoms.start.Start
          type: Start
          uuid: 242f6190-9b37-11e6-950d-a24fc0d9651c
        Stop:
          enabled: true
          inputs:
            mandatory:
              - Volume.volname
          name: stop_volume
          run: tendrl.gluster_integration.objects.Volume.atoms.stop.Stop
          type: Stop
          uuid: 242f6190-9b37-11e6-950d-a24fc0d9652c
        NamedVolumeNotExists:
          enabled: true
          inputs:
            mandatory:
              - Volume.volname
          name: named volume not exists
          run: tendrl.gluster_integration.objects.Volume.atoms.named_volume_not_exists.NamedVolumeNotExists
          type: Check
          uuid: 242f6190-9b37-11e6-950d-a24fc0d9653d
        VolumeExists:
          enabled: true
          inputs:
            mandatory:
              - Volume.vol_id
          name: volume_exists
          run: tendrl.gluster_integration.objects.Volume.atoms.volume_exists.VolumeExists
          type: Check
          uuid: 242f6190-9b37-11e6-950d-a24fc0d9653c
        VolumeNotExists:
          enabled: true
          inputs:
            mandatory:
              - Volume.vol_id
          name: volume_not_exists
          run: tendrl.gluster_integration.objects.Volume.atoms.volume_not_exists.VolumeNotExists
          type: Check
          uuid: 242f6190-9b37-11e6-950d-a24fc0d9654c
        VolumeStarted:
          enabled: true
          inputs:
            mandatory:
              - Volume.vol_id
          name: volume_started
          run: tendrl.gluster_integration.objects.Volume.atoms.volume_started.VolumeStarted
          type: Check
          uuid: 242f6190-9b37-11e6-950d-a24fc0d9655c
        VolumeStopped:
          enabled: true
          inputs:
            mandatory:
              - Volume.vol_id
          name: volume_stopped
          run: tendrl.gluster_integration.objects.Volume.atoms.volume_stopped.VolumeStopped
          type: Check
          uuid: 242f6190-9b37-11e6-950d-a24fc0d9656c
        RebalanceNotRunning:
          enabled: true
          inputs:
            mandatory:
              - Volume.volname
              - Volume.vol_id
          name: rebalancenotrunning
          run: tendrl.gluster_integration.objects.Volume.atoms.rebalance_not_running.RebalanceNotRunning
          type: Check
          uuid: 242f6190-9b37-11e6-950d-a24fc0d96588
        StartRebalance:
          enabled: true
          inputs:
            mandatory:
              - Volume.volname
              - Volume.vol_id
            optional:
              - Volume.fix_layout
          name: start_rebalance
          run: tendrl.gluster_integration.objects.Volume.atoms.start_rebalance.StartRebalance
          type: Start
          uuid: 242f6190-9b37-11e6-950d-a24fc0d96599
      flows:
        DeleteVolume:
          atoms:
            - tendrl.gluster_integration.objects.Volume.atoms.delete.Delete
          help: "Delete Volume"
          enabled: true
          inputs:
            mandatory:
              - Volume.volname
              - Volume.vol_id
          post_run:
            - tendrl.gluster_integration.objects.Volume.atoms.volume_not_exists.VolumeNotExists
          pre_run:
            - tendrl.gluster_integration.objects.Volume.atoms.volume_exists.VolumeExists
          run: tendrl.gluster_integration.objects.Volume.flows.delete_volume.DeleteVolume
          type: Delete
          uuid: 1951e821-7aa9-4a91-8183-e73bc8275b9e
          version: 1
        StartVolume:
          atoms:
            - tendrl.gluster_integration.objects.Volume.atoms.start.Start
          help: "Start Volume"
          enabled: true
          inputs:
            mandatory:
              - Volume.volname
              - Volume.vol_id
          pre_run:
            - tendrl.gluster_integration.objects.Volume.atoms.volume_exists.VolumeExists
            - tendrl.gluster_integration.objects.Volume.atoms.volume_stopped.VolumeStopped
          run: tendrl.gluster_integration.objects.Volume.flows.start_volume.StartVolume
          type: Start
          uuid: 1951e821-7aa9-4a91-8183-e73bc8275b6e
          version: 1
        StopVolume:
          atoms:
            - tendrl.gluster_integration.objects.Volume.atoms.stop.Stop
          help: "Stop Volume"
          enabled: true
          inputs:
            mandatory:
              - Volume.volname
              - Volume.vol_id
          pre_run:
            - tendrl.gluster_integration.objects.Volume.atoms.volume_exists.VolumeExists
            - tendrl.gluster_integration.objects.Volume.atoms.volume_started.VolumeStarted
          run: tendrl.gluster_integration.objects.Volume.flows.stop_volume.StopVolume
          type: Stop
          uuid: 1951e821-7aa9-4a91-8183-e73bc8275b5e
          version: 1
        StartVolumeRebalance:
          atoms:
            - tendrl.gluster_integration.objects.Volume.atoms.start_rebalance.StartRebalance
          help: "Start Volume Rebalance"
          enabled: true
          inputs:
            mandatory:
              - Volume.volname
              - Volume.vol_id
            optional:
              - Volume.fix_layout
          pre_run:
            - tendrl.gluster_integration.objects.Volume.atoms.volume_exists.VolumeExists
            - tendrl.gluster_integration.objects.Volume.atoms.volume_started.VolumeStarted
            - tendrl.gluster_integration.objects.Volume.atoms.rebalance_not_running.RebalanceNotRunning
          run: tendrl.gluster_integration.objects.Volume.flows.start_volume_rebalance.StartVolumeRebalance
          type: Start
          uuid: 1951e821-7aa9-4a91-8183-e73bc8275b7d
          version: 1
      attrs:
        arbiter_count:
          help: "Arbiter count of volume"
          type: Integer
        bricks:
          help: "List of brick mnt_paths for volume"
          type: List
        deleted:
          help: "Flag is volume is deleted"
          type: Boolean
        disperse_count:
          help: "Disperse count of volume"
          type: Integer
        disperse_data_count:
          help: "Disperse data count of volume"
          type: Integer
        force:
          help: "If force execute the action"
          type: Boolean
        redundancy_count:
          help: "Redundancy count of volume"
          type: Integer
        replica_count:
          help: "Replica count of volume"
          type: Integer
        stripe_count:
          help: "Stripe count of volume"
          type: Integer
        transport:
          help: "Transport type for volume"
          type: String
        vol_id:
          help: "ID of the gluster volume"
          type: String
        volname:
          help: "Name of gluster volume"
          type: String
        vol_type:
          help: "Type of the volume"
          type: String
        rebal_data:
          help: "Rebalance data"
          type: String
        rebal_skipped:
          help: "Skipped files while rebalance"
          type: String
        subvol_count:
          help: "Count of subvolumes"
          type: Integer
        brick_count:
          help: "Count of bricks"
          type: Integer
        cluster_id:
          help: "UUID of the cluster"
          type: String
        snapd_inited:
          help: "If snapd is initialized"
          type: String
        status:
          help: "Status of the volume"
          type: String
        rebal_id:
          help: "UUID of the rabalance task"
          typoe: String
        rebal_lookedup:
          help: "Looked up files for rebalance"
          type: Integer
        snap_count:
          help: "Count of the snapshots"
          type: Integer
        rebal_files:
          help: "No of files rebalanced"
          type: Integer
        snapd_status:
          help: "Status of snapd"
          type: String
        options:
          help: "options list for volume"
          type: json
        quorum_status:
          help: "Quorum status"
          type: String
        rebal_status:
          help: "Status of rebalance task"
          type: String
        rebal_failures:
          help: "Failed no of files for rebalance"
          type: Integer
      enabled: true
      value: clusters/$TendrlContext.integration_id/Volumes/$Volume.vol_id/
      list: clusters/$TendrlContext.integration_id/Volumes
      help: gluster volume
tendrl_schema_version: 0.3
"""
