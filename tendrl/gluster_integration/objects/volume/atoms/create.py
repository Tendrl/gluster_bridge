import subprocess


class Create(object):
    def start(
        self,
        name,
        bricks,
        replica_count=None,
        arbiter_count=None,
        stripe_count=None,
        disperse_count=None,
        disperse_data_count=None,
        redundancy_count=None,
        transport=[],
        ):
        cmd = ['gluster', 'volume', 'create', name]
        if stripe_count is not None:
            cmd.append('stripe')
            cmd.append(str(stripe_count))
        elif replica_count is not None:
            cmd.append('replica')
            cmd.append(str(replica_count))
            if arbiter_count is not None:
                cmd.append('arbiter')
                cmd.append(str(arbiter_count))
        elif disperse_count is not None:
            cmd.append('disperse')
            cmd.append(str(disperse_count))
        elif redundancy_count is not None:
            cmd.append('redundancy')
            cmd.append(str(redundancy_count))
        elif disperse_data_count is not None:
            cmd.append('disperse-data')
            cmd.append(str(disperse_data_count))
        if transport:
            cmd.append('transport')
            cmd.append(','.join(transport))
        cmd.extend(bricks)
        cmd.append('force')
        cmd.append('--mode=script')
        subprocess.call(cmd)
        subprocess.call(['gluster', 'volume', 'start',
                        name, '--mode=script'])
