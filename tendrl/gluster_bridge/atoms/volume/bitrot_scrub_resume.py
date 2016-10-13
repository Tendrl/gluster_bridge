import subprocess


class BitrotScrubResume(object):
    def start(self, name):
        subprocess.call(['gluster', 'volume', 'bitrot',
                        name, 'scrub', 'resume'])
