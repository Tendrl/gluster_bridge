import subprocess


class AddBrick(object):
    def start(self, name, brick):
        subprocess.call(['gluster', 'volume', 'add-brick',
                         name, brick])
