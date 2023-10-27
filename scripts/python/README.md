# Python scripts for managing an SCR job

These scripts define an ``scrjob`` package that can be used to manage an SCR job.
After installing SCR, the package is installed to ``/libexec/python/scrjob``.

NOTE: Though a ``setup.py`` exists, it is not currently functional.

# Commands installed to ``/libexec``

The files in this directory are installed to ``/libexec/python`` of an SCR installation.
They are usually called by other SCR commands.
These are not typically invoked by a user,
though they may be helpful for debugging and testing.

- ``scr_check_node.py``    - Executed on each compute node to check its health
- ``scr_ckpt_interval.py`` - Given an SCR log, compute estimate for optimal interval between checkpoints
- ``scr_env.py``           - Print various values from an allocation environment
- ``scr_glob_hosts.py``    - Manipulate a hostlist string, to expand, compress, and subtract nodes
- ``scr_inspect.py``       - Executed on each compute node to determine whether a dataset can be scavenged
- ``scr_kill_jobstep.py``  - Given the specified launcher and jobstepid, call ``Joblauncher.kill_jobstep(jobstepid)``
- ``scr_list_dir.py``      - Prints the current SCR control or cache directories
- ``scr_poststage.py``     - Intended to be called to initiate a poststage operation where supported (non-functional)
- ``scr_scavenge.py``      - Execute a scavenge operation to copy files from cache to the prefix directory

Commands copied to ``/libexec/python`` refer to the ``scrjob`` package via relative path.

# Commands installed to ``/bin``

The scripts in ``commands`` are installed to ``/bin`` of an SCR installation.
These are described in the user documentation,
and they are executed directly by the user.
Commands copied to ``/bin`` hardcode the path to the ``scrjob`` package at install time.
See the [README](commands/README.md) for more information.

# Testing

The ``tests`` directory implements tests for the ``scrjob`` package.
These are copied to ``libexec/python/tests`` of an SCR installation.
See the [README](tests/README.md) for more information.

To execute the tests, first acquire a two-node allocation.

Then, ensure these variables at the top of ``runtest.sh`` are appropriate values:
- ``launcher``
- ``numnodes``
- ``MPICC``

There is a ``sleep`` in ``scr_run.py`` which can be reduced for testing.

From an allocation, run the test script::

    cd libexec/python/tests
    ./runtest.sh

To add additional test scripts, place a file whose name matches ``test*.py``.

# ClusterShell (optional)

The ``scrjob`` package can use the ClusterShell module if available.
This can be disabled by setting ``USE_CLUSTERSHELL='0'`` in ``scrjob/config.py``.
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

## TODO

Node groups can be bound together by differing lists.

Library defaults may need to be overridden for identifying nodes
(see bottom of the config.html link)

The ClusterShell``NodeSet`` class supports more operations than ``scr_hostlist``.
See the NodeSet class: ``ClusterShell.NodeSet.NodeSet``.
Using package: clustershell.readthedocs.io/en/latest/guide/taskmgnt.html
