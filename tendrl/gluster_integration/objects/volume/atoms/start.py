import subprocess


class Start(object):
    def start(self, name):
        subprocess.call(['gluster', 'volume', 'start',
                        name, 'force', '--mode=script'])
