import subprocess


class ReplaceBrickAbort(object):
    def start(self, name, brick, new_brick):
        subprocess.call(['gluster', 'volume', 'replace-brick',
                         name, brick, new_brick, 'abort'])
