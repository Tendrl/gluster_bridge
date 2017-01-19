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
      post_run:
        - tendrl.gluster_integration.objects.Volume.atoms.volume_exists.VolumeExists
      pre_run:
        - tendrl.gluster_integration.objects.Volume.atoms.volume_not_exists.VolumeNotExists
      run: tendrl.gluster_integration.flows.create_volume.CreateVolume
      type: Create
      uuid: 1951e821-7aa9-4a91-8183-e73bc8275b8e
      version: 1
  objects:
    Peers:
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
      value: clusters/$Tendrl_context.cluster_id/Peers/$Peer.peer_uuid
    Tendrl_context:
      attrs:
        cluster_id:
          help: "Tendrl managed/generated cluster id for the sds being managed by Tendrl"
          type: String
        sds_name:
          help: gluster
          type: String
        sds_version:
          help: "3.8.3"
          type: String
      enabled: true
      value: clusters/$Tendrl_context.cluster_id/Tendrl_context/
    Volume:
      atoms:
        create:
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
        delete:
          enabled: true
          inputs:
            mandatory:
              - Volume.volname
              - Volume.vol_id
          name: delete_volume
          run: tendrl.gluster_integration.objects.Volume.atoms.delete.Delete
          type: Delete
          uuid: 242f6190-9b37-11e6-950d-a24fc0d9650c
        start:
          enabled: true
          inputs:
            mandatory:
              - Volume.volname
          name: start_volume
          run: tendrl.gluster_integration.objects.Volume.atoms.start.Start
          type: Start
          uuid: 242f6190-9b37-11e6-950d-a24fc0d9651c
        stop:
          enabled: true
          inputs:
            mandatory:
              - Volume.volname
          name: stop_volume
          run: tendrl.gluster_integration.objects.Volume.atoms.stop.Stop
          type: Stop
          uuid: 242f6190-9b37-11e6-950d-a24fc0d9652c
        volume_exists:
          enabled: true
          inputs:
            mandatory:
              - Volume.vol_id
          name: volume_exists
          run: tendrl.gluster_integration.objects.Volume.atoms.volume_exists.VolumeExists
          type: Check
          uuid: 242f6190-9b37-11e6-950d-a24fc0d9653c
        volume_not_exists:
          enabled: true
          inputs:
            mandatory:
              - Volume.vol_id
          name: volume_not_exists
          run: tendrl.gluster_integration.objects.Volume.atoms.volume_not_exists.VolumeNotExists
          type: Check
          uuid: 242f6190-9b37-11e6-950d-a24fc0d9654c
        volume_started:
          enabled: true
          inputs:
            mandatory:
              - Volume.vol_id
          name: volume_started
          run: tendrl.gluster_integration.objects.Volume.atoms.volume_started.VolumeStarted
          type: Check
          uuid: 242f6190-9b37-11e6-950d-a24fc0d9655c
        volume_stopped:
          enabled: true
          inputs:
            mandatory:
              - Volume.vol_id
          name: volume_stopped
          run: tendrl.gluster_integration.objects.Volume.atoms.volume_stopped.VolumeStopped
          type: Check
          uuid: 242f6190-9b37-11e6-950d-a24fc0d9656c
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
          post_run:
            - tendrl.gluster_integration.objects.Volume.atoms.volume_started.VolumeStarted
          pre_run:
            - tendrl.gluster_integration.objects.Volume.atoms.volume_exists.VolumeExists
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
          post_run:
            - tendrl.gluster_integration.objects.Volume.atoms.volume_stopped.VolumeStopped
          pre_run:
            - tendrl.gluster_integration.objects.Volume.atoms.volume_exists.VolumeExists
          run: tendrl.gluster_integration.objects.Volume.flows.stop_volume.StopVolume
          type: Stop
          uuid: 1951e821-7aa9-4a91-8183-e73bc8275b5e
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
      value: clusters/$Tendrl_context.cluster_id/Volumes/$Volume.vol_id/
tendrl_schema_version: 0.3
"""
