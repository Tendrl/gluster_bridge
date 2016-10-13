import subprocess


class RebalanceStop(object):
    def start(self, name):
        subprocess.call(['gluster', 'volume', 'rebalance',
                        name, 'stop'])
