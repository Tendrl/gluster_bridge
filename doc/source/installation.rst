===========
Environment
===========

1. Install Gluster current stable release (https://www.gluster.org/download/)
2. Install Etcd>=2.3.x && <3.x (https://github.com/coreos/etcd/releases/tag/v2.3.7)


============
Installation
============

Since there is no stable release yet, the only option is to install the project from the source.

Development version from the source
-----------------------------------

1. Install http://github.com/tendrl/common from the source code::

    $ git clone https://github.com/Tendrl/common.git
    $ cd common
    $ mkvirtualenv common
    $ pip install .

2. Install gluster-integration itself::

    $ git clone https://github.com/Tendrl/gluster-integration.git
    $ cd gluster-integration
    $ workon gluster-integration
    $ pip install .

Note that we use virtualenvwrapper_ here to activate ``gluster-integration`` `python
virtual enviroment`_. This way, we install *gluster integration* into the same virtual
enviroment which we have created during installation of *integration common*.

.. _virtualenvwrapper: https://virtualenvwrapper.readthedocs.io/en/latest/
.. _`python virtual enviroment`: https://virtualenv.pypa.io/en/stable/

3. Create config file::

    $ cp etc/tendrl/gluster-integration/gluster-integration.conf.yaml.sample /etc/tendrl/gluster-integration/gluster-integration.conf.yaml

4. Configure the etcd ip in (/etc/tendrl/gluster-integration/gluster-integration.conf.yaml) by changing the following line ::

    etcd_connection: <Specify etcd server ip here>

5. Run::

    $ tendrl-gluster-integration
