import subprocess


class BitrotScrubPause(object):
    def start(self, name):
        subprocess.call(['gluster', 'volume', 'bitrot',
                        name, 'scrub', 'pause'])
