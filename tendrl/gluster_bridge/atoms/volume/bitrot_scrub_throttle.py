import subprocess


class BitrotScrubThrottle(object):
    def start(self, name, mode):
        subprocess.call(['gluster', 'volume', 'bitrot',
                        name, 'scrub-throttle', mode])
