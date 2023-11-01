# Remote command execution classes
SCR needs to execute remote commands on the compute nodes of the allocation.
This is used for running node health checks to detect failed nodes
and for executing scavenge operations to copy datasets from cache.
The precise way this is accomplished varies by system and HPC center.
This directory contains various implementations for remote command execution.

Base class:
- ``remoteexec.py`` - Defines the ``RemoteExec`` and ``RemoteExecResult`` classes

Existing remote execution classes:
- ``clustershell.py`` - Runs remote command using the ClusterShell Python API
- ``flux.py`` - Runs remote command by submitting a Flux job
- ``pdsh.py`` - Runs remote command via a ``pdsh`` command

# Adding a new remote execution method

The steps below describe how to add a new remote execution class.

## Define a new remote execution class
One can add support for a new remote execution class by extending
the `RemoteExec` class and implementing the required interface.
See the `RemoteExec` class in `remoteexec.py`
for the interface definitions that one must implement, e.g.:

    >>: cat newrexec.py
    from scrjob.remoteexec import RemoteExec

    class NewRexec(RemoteExec):
      def rexec(argv, nodes, jobenv):
        pass

The ``rexec`` method must return a ``RemoteExecResult`` object.

## Import the new class in `__init__.py`
Add a line to import the new class in the `__init__.py` file
after the ``RemoteExec`` import:

    from .remoteexec import RemoteExec
    ...
    from .newrexec import NewRexec

## Add class file to `CMakeLists.txt`
Include the new class to the list of files to be installed by CMake in `CMakeLists.txt`:

    SET(SCRIPTS
      ...
      newrexec.py
    )
