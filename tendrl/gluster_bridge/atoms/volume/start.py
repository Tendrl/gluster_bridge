import subprocess


class Start(object):
    def start(self, name, bricks):
        subprocess.call(['gluster', 'volume', 'start', name])
