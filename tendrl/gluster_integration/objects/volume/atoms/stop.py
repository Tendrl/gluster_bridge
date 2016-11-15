import subprocess


class Stop(object):
    def start(self, name):
        subprocess.call(['gluster', 'volume', 'stop',
                        name, 'force', '--mode=script'])
