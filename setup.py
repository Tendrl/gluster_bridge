import re
from setuptools import Command
from setuptools import find_packages
from setuptools import setup
import subprocess
try:
    # Python 2 backwards compat
    from __builtin__ import raw_input as input
except ImportError:
    pass


def read_module_contents():
    with open('version.py') as app_init:
        return app_init.read()


def read_spec_contents():
    with open('tendrl-gluster-integration.spec') as spec:
        return spec.read()

module_file = read_module_contents()
metadata = dict(re.findall("__([a-z]+)__\s*=\s*'([^']+)'", module_file))
version = metadata['version']


class BumpVersionCommand(Command):
    """Bump the __version__ number and commit all changes."""

    user_options = [('version=', 'v', 'version number to use')]

    def initialize_options(self):
        new_version = metadata['version'].split('.')
        new_version[-1] = str(int(new_version[-1]) + 1)  # Bump the final part
        self.version = ".".join(new_version)

    def finalize_options(self):
        pass

    def run(self):

        print('old version: %s  new version: %s' %
              (metadata['version'], self.version))
        try:
            input('Press enter to confirm, or ctrl-c to exit >')
        except KeyboardInterrupt:
            raise SystemExit("\nNot proceeding")

        old = "__version__ = '%s'" % metadata['version']
        new = "__version__ = '%s'" % self.version
        module_file = read_module_contents()
        with open('version.py', 'w') as fileh:
            fileh.write(module_file.replace(old, new))

        old = 'Version: %s' % metadata['version']
        new = 'Version: %s' % self.version
        spec_file = read_spec_contents()
        with open('tendrl-gluster-integration.spec', 'w') as fileh:
            fileh.write(spec_file.replace(old, new))

        # Commit everything with a standard commit message
        cmd = ['git', 'commit', '-a', '-m', 'version %s' % self.version]
        print(' '.join(cmd))
        subprocess.check_call(cmd)


class ReleaseCommand(Command):
    """Tag and push a new release."""

    user_options = [('sign', 's', 'GPG-sign the Git tag and release files')]

    def initialize_options(self):
        self.sign = False

    def finalize_options(self):
        pass

    def run(self):
        # Create Git tag
        tag_name = 'v%s' % version
        cmd = ['git', 'tag', '-a', tag_name, '-m', 'version %s' % version]
        if self.sign:
            cmd.append('-s')
        print(' '.join(cmd))
        subprocess.check_call(cmd)

        # Push Git tag to origin remote
        cmd = ['git', 'push', 'origin', tag_name]
        print(' '.join(cmd))
        subprocess.check_call(cmd)

setup(
    name="tendrl-gluster-integration",
    version=version,
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*",
                                    "tests"]),
    namespace_packages=['tendrl'],
    url="http://www.redhat.com",
    author="Rohan Kanade.",
    author_email="rkanade@redhat.com",
    license="LGPL-2.1+",
    zip_safe=False,
    install_requires=[
        "tendrl-commons",
        "flask",
        "netifaces",
        "cherrypy",
        "paste"
        ],
    entry_points={
        'console_scripts': [
            'tendrl-gluster-integration = tendrl.gluster_integration.manager:main',
            'tendrl-gluster-vol-utilization = tendrl.gluster_integration.sds_sync.vol_utilization:main'
        ]
    },
    include_package_data=True,
    cmdclass={'bumpversion': BumpVersionCommand, 'release': ReleaseCommand},
)
