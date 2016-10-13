import subprocess


class RemoveBrickCommit(object):
    def start(self, name, brick):
        subprocess.call(['gluster', 'volume', 'remove-brick',
                         name, brick, 'commit'])
