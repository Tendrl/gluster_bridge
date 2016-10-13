import subprocess


class BitrotScrubFrequency(object):
    def start(self, name, frequency):
        subprocess.call(['gluster', 'volume', 'bitrot',
                        name, 'scrub-frequency', frequency])
