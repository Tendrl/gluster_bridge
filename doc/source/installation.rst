===========
Environment
===========

1. Install Gluster current stable release (https://www.gluster.org/download/)
2. Install Etcd>=2.3.x && <3.x (https://github.com/coreos/etcd/releases/tag/v2.3.7)


============
Installation
============

Since there is no stable release yet, the only option is to install the project
from the source.

Development version from the source
-----------------------------------

1. Install http://github.com/tendrl/bridge_common from the source code::

    $ git clone https://github.com/Tendrl/bridge_common.git
    $ cd bridge_common
    $ mkvirtualenv gluster_bridge
    $ pip install .

2. Install gluster bridge itself::

    $ git clone https://github.com/Tendrl/gluster_bridge.git
    $ cd gluster_bridge
    $ workon gluster_bridge
    $ pip install .

Note that we use virtualenvwrapper_ here to activate ``gluster_bridge`` `python
virtual enviroment`_.  This way, we install *gluster bridge* into the same
virtual enviroment which we have created during installation of *bridge
common*.

.. _virtualenvwrapper: https://virtualenvwrapper.readthedocs.io/en/latest/
.. _`python virtual enviroment`: https://virtualenv.pypa.io/en/stable/

3. Create config file::

    $ cp etc/tendrl/tendrl.conf.sample /etc/tendrl/tendrl.conf

4. Edit ``/etc/tendrl/tendrl.conf`` as required

5. Create log dir::

    $ mkdir /var/log/tendrl

6. Run::

    $ tendrl-gluster-bridge
