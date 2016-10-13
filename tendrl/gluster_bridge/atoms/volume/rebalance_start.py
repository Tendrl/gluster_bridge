import subprocess


class RebalanceStart(object):
    def start(self, name, force, fix_layout):
        cmd = ['gluster', 'volume', 'rebalance', name]
	if fix_layout:
            cmd.append('fix-layout')
        cmd.append('start')
        if force:
            cmd.append('force')
        subprocess.call(cmd)
