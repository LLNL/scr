# The ``scrjob`` package manages SCR jobs

The ``scrjob`` package implements a python interface for managing an SCR job.
This includes support for an SCR job within an active resource allocation
and managing datasets written by a previous SCR job.
Typically, users invoke methods from this package indirectly through
commands in the ``/bin`` directory of an SCR installation.
Users with python-based job scripts may find value in directly using the ``scrjob`` package.

- ``/cli``               - Wrappers for certain SCR executables
- ``/launchers``         - MPI job launcher classes
- ``/nodetests``         - Node health check classes
- ``/remoteexec``        - Remote command execution classes
- ``/resmgrs``           - Resource manager classes

- ``common.py``          - Functions for running subprocess commands
- ``config.py.in``       - Captures values selected during configuration
- ``hostlist.py``        - Functions for manipulating a list of nodes
- ``jobenv.py``          - JobEnv: provides access to SCR parameters, resource manager, and job launcher
- ``list_down_nodes.py`` - Function to detect and report down nodes in an allocation
- ``param.py``           - Param: provides access to SCR parameters set in the environment or SCR config files
- ``parsetime.py``       - Functions to parse time strings used in ``scr_halt`` commands
- ``postrun.py``         - Function to execute after the final run in an allocation, calls scavenge
- ``prerun.py``          - Function to execute before first run in an allocation
- ``run.py``             - Function to wrap multiple runs in an allocation, calls prerun, postrun, list\_down\_nodes, etc.
- ``scavenge.py``        - Function to scavenge datasets from SCR cache to the prefix directory
- ``scrlog.py``          - Functions to parse entries in an SCR log file
- ``should_exit.py``     - Function to determine whether one should stop running SCR jobs in an allocation
- ``test_runtime.py``    - TestRuntime: checks that certain dependencies exist
- ``watchdog.py``        - Watchdog: launches a command, monitors its progress, and kills if a timeout expires

# Extending ``scrjob`` to support new resource managers, job launchers, and node health checks

SCR supports a few common resource managers (SLURM, LSF, and Flux) and MPI job launchers (``srun``, ``jsrun``, ``flux run``).
It executes a few simple tests to detect failed nodes.
One can add support for new resource managers, job launchers, and node health checks to customize SCR for different systems.

To add a new resource manager, see [resmgrs/README](resmgrs/README.md).

To add a new MPI job launcher, see [launchers/README](launchers/README.md).

To add a new node health check, see [nodetests/README](nodetests/README.md).
