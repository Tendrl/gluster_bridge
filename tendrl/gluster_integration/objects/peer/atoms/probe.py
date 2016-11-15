import subprocess


class Probe(object):
    def start(self, peer):
        subprocess.call(['gluster', 'peer', 'probe',
                        peer, '--mode=script'])
