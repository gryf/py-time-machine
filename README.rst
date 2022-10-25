py-time-machine
===============

Rsync/hardlinks based python script for backup purposes using local or remote
locations. Inspired by `rsync-time-machine`_.

Features
--------

Same as in `rsync-time-machine`_, but also:

* Python3 support
* Backup to remote host
* Configuration will be read from ``XDG_CONFIG_HOME`` or ``/etc/``
* Using yaml instead of ini style config files


Usage
-----

Place the script on your path, create configuration and save it under
convenient location.

Than, execute it:

.. code:: shell-session

   $ py-time-machine.py

or provide configuration file:


.. code:: shell-session

   $ py-time-machine.py -c file_with_configuration.yaml


Configuration
-------------

Sample configuration can be found with this repository with comments. To
configure remote host, it's as simple as:

.. code:: yaml

   ...
   dest: user@hostname_or_ip_address:/path/to/the/backup
   ...

In this example ``user@`` might be omitted, so that current user would be used.
Please note, that in case of remote destination, absolute path should be used.


License
-------

See the LICENSE file for license rights and limitations (GNU GPL v2).


.. _rsync-time-machine: https://github.com/infinet/rsync-time-machine
