import subprocess


class SetVolumeOption(object):
    def start(self, name, option, option_value):
        subprocess.call(['gluster', 'volume', 'set',
                         name, option, option_value])
