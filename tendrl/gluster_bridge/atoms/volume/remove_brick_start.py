import subprocess


class RemoveBrickStart(object):
    def start(self, name, brick):
        subprocess.call(['gluster', 'volume', 'remove-brick',
                         name, brick, 'start'])
