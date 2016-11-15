import subprocess


class AddBrick(object):
    def start(self, name, bricks):
        cmd = ['gluster', 'volume', 'add-brick', name]
        cmd.extend(bricks)
        cmd.append('force')
        cmd.append('--mode=script')
        subprocess.call(cmd)
