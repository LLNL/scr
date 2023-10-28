# The ``scrjob`` package manages SCR jobs

The ``scrjob`` package implements a python interface for managing an SCR job.
This includes support for an SCR job within an active resource allocation,
as well as managing datasets written by a previous SCR job.
Typically, users invoke methods from this package indirectly through the various
commands in the ``/bin`` directory of an SCR installation.
Users with python-based job scripts may find value in directly using the ``scrjob`` package.

- ``/cli``               - Wrappers for certain SCR executables
- ``/launchers``         - MPI job launcher classes
- ``/resmgrs``           - Resource manager classes
- ``common.py``          - Functions for running subprocess commands
- ``config.py.in``       - Captures values selected during configuration
- ``environment.py``     - JobEnv: provides access to SCR parameters, resource manager, and job launcher
- ``glob_hosts.py``      - Functions for manipulating a hostlist string
- ``hostlist.py``        - Functions for manipulating a list of nodes
- ``list_dir.py``        - Functions to lookup SCR control and cache directories
- ``list_down_nodes.py`` - Function to detect and report down nodes in an allocation
- ``param.py``           - Param: provides access to SCR parameters set in the environment or SCR config files
- ``parsetime.py``       - Functions to parse time strings used in ``scr_halt`` commands
- ``postrun.py``         - Function to execute after the final run in an allocation, calls scavenage
- ``prerun.py``          - Function to execute before first run in an allocation
- ``run.py``             - Function to wrap multiple runs in an allocation, calls prerun, postrun, list\_down\_nodes, etc.
- ``scavenge.py``        - Function to scavenge datasets from SCR cache to the prefix directory
- ``scrlog.py``          - Functions to parse entries in an SCR log file
- ``should_exit.py``     - Function to determine whether one should stop running SCR jobs in an allocation
- ``test_runtime.py``    - TestRuntime: checks that certain dependencies exist
- ``watchdog.py``        - Watchdog: launches a command, monitors its progress, and kills if a timeout expires

SCR supports a few common resource managers (SLURM, LSF, and Flux) and MPI job launchers (``srun``, ``jsrun``, ``flux run``).
To add a new resource manager, see [resmgrs/README](resmgrs/README.md).
To add a new MPI job launcher, see [launchers/README](launchers/README.md).
