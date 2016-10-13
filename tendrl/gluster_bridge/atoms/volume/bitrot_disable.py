import subprocess


class BitrotDisable(object):
    def start(self, name):
        subprocess.call(['gluster', 'volume', 'bitrot',
                        name, 'disable'])
