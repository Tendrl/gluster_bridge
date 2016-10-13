import subprocess


class Delete(object):
    def start(self, name):
        subprocess.call(['gluster', 'volume', 'delete', name])
