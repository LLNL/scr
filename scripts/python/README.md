# Python scripts for managing an SCR job

These scripts define and use an ``scrjob`` package to manage an SCR job.
Most files in this directory are installed to ``/libexec/python`` of an SCR installation.

NOTE: Though a ``setup.py`` exists, it is not currently functional.

- ``/commands`` - scripts that use ``scrjob``, installed to ``/bin`` of an SCR installation
- ``/scrjob`` - ``scrjob`` package files
- ``/tests`` - tests for the ``scrjob`` package

The following scripts are not typically invoked by a user,
and they are not considered to be part of the SCR user interface.
However, these scripts are helpful for debugging and testing.

- ``scr_check_node.py``    - Execute on each compute node to check access to cache and control directories
- ``scr_ckpt_interval.py`` - Given an SCR log file, compute estimate for optimal interval between checkpoints
- ``scr_env.py``           - Print various values from an allocation environment
- ``scr_hostlist.py``      - Manipulate a hostlist string, to expand, compress, and subtract nodes
- ``scr_inspect.py``       - Execute on each compute node to determine whether a dataset can be scavenged (not used)
- ``scr_kill_jobstep.py``  - Given the specified launcher and jobstepid, call ``JobLauncher.kill_jobstep(jobstepid)``
- ``scr_list_dir.py``      - Print the SCR control or cache directories
- ``scr_poststage.py``     - Initiate a poststage operation where supported (non-functional)
- ``scr_scavenge.py``      - Execute a scavenge operation to copy files from cache to the prefix directory

# ClusterShell (optional)

The ``scrjob`` package can use the ClusterShell module if available.
This can be enabled by setting ``USE_CLUSTERSHELL = True`` in ``scrjob/config.py``.
ClusterShell is not used if it is not found or if it is disabled.

ClusterShell is useful as:
- an ``scr_hostlist`` replacement for manipulating host lists
- a ``pdsh`` replacement for running commands on a set of compute nodes

To install:

```
pip install ClusterShell
```

To configure ClusterShell, see:
- [ClusterShell config docs](https://clustershell.readthedocs.io/en/latest/config.html)
- ``man clush.conf``

Node groups can be bound together by differing lists.

Library defaults may need to be overridden for identifying nodes
(see bottom of the config.html link)

The ClusterShell``NodeSet`` class supports more operations than ``scr_hostlist``.
See the NodeSet class: ``ClusterShell.NodeSet.NodeSet``.
Using package: clustershell.readthedocs.io/en/latest/guide/taskmgnt.html
