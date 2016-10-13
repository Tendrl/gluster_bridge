import subprocess


class BitrotEnable(object):
    def start(self, name):
        subprocess.call(['gluster', 'volume', 'bitrot',
                        name, 'enable'])
