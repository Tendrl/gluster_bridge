import subprocess


class Detach(object):
    def start(self, peer):
        subprocess.call(['gluster', 'peer', 'detach',
                        peer, '--mode=script'])
