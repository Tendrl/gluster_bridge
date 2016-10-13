import subprocess


class LogRotate(object):
    def start(self, name):
        subprocess.call(['gluster', 'volume', 'log',
                        'rotate', name])
