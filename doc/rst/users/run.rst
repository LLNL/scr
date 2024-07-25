.. _sec-scripts:

Run a job
=========

In addition to the SCR library,
one may optionally include SCR commands in a job script.
These commands are most useful on systems where failures are common.
The SCR commands prepare the cache, scavenge files from cache to the parallel file system,
and check that the scavenged dataset is complete among other things.
The commands also automate the process of relaunching a job after failure,
including logic to detect to exclude failed nodes.

There are several SCR commands,
most of which are located in the :code:`/bin` directory where SCR is installed.

For best performance, one should:
1) inform the batch system that the allocation should remain available even after a failure
and 2) replace the command to execute the application with an SCR wrapper script.
The precise set of options and commands to use depends on the system resource manager.

Supported platforms
-------------------

At the time of this writing, SCR supports specific combinations of resource managers and job launchers.
The descriptions for using SCR in this section apply to
these specific configurations,
however the following description is helpful to understand
how to run SCR on any system.
Please contact us for help in porting SCR to other platforms.
(See Section :ref:`sec-contact` for contact information).

Jobs and job steps
------------------
First, we differentiate between a *job allocation* and a *job step*.
Our terminology originates from the SLURM resource manager, but
the concepts apply generally across other resource managers.

When a job is scheduled resources on a system,
the batch script executes inside of a job allocation.
The job allocation consists of a set of nodes, a time limit, and a job id.
The job id can be obtained by executing the :code:`squeue` command
on SLURM, the :code:`apstat` command on ALPS, and the :code:`bjobs` command on LSF.

Within a job allocation, a user may run one or more job steps.
One runs a job step using :code:`srun` on SLURM, :code:`aprun` on ALPS, or :code:`jsrun` on LSF.
Each job step is assigned its own step id.

On SLURM, job step ids start at 0 and increment with each issued job step.
Job step ids can be obtained by passing the :code:`-s` option to :code:`squeue`.
A fully qualified name of a SLURM job step consists of: :code:`jobid.stepid`.
For instance, the name :code:`1234.5` refers to step id 5 of job id 1234.

On ALPS, each job step has a unique id that can be obtained
through :code:`apstat`.

Tolerating node failures
------------------------

Before running an SCR job, it is recommended to configure the job allocation to withstand node failures.
By default, most resource managers terminate the job allocation if a node fails.
SCR requires the job allocation to remain active in order to restart the job or to scavenge files.
To enable the job allocation to continue past node failures,
one must specify the appropriate flags from the table below.

SCR job allocation flags

================== ================================================================
LSF batch script   :code:`#BSUB -env "all, LSB_DJOB_COMMFAIL_ACTION=KILL_TASKS"`
LSF interactive    :code:`bsub -env "all, LSB_DJOB_COMMFAIL_ACTION=KILL_TASKS" ...`
MOAB batch script  :code:`#MSUB -l resfailpolicy=ignore`
MOAB interactive   :code:`qsub -I ... -l resfailpolicy=ignore`
SLURM batch script :code:`#SBATCH --no-kill`
SLURM interactive  :code:`salloc --no-kill ...`
================== ================================================================

The SCR wrapper scripts
-----------------------
The easiest way to integrate SCR into a batch script is to set some environment variables
and to replace the job run command with an SCR wrapper script.

Example bash job scripts are located in the :code:`share/examples` directory of an SCR installation::

  SLURM:  scr_srun.sh [srun_options]  <prog> [prog_args ...]
  SLURM:  scr_srun_loop.sh [srun_options]  <prog> [prog_args ...]
  LSF:    scr_jsrun.sh [jsrun_options] <prog> [prog_args ...]
  LSF:    scr_jsrun_loop.sh [jsrun_options] <prog> [prog_args ...]

These scripts are customized for different resource managers.
They include the resource manager flag that one needs to set to tolerate node failures.
They call :code:`scr_prerun` to prepare an allocation before starting a run,
they launch a run with :code:`srun` or :code:`jsrun`,
and they call :code:`scr_postrun` to scavange any cached datasets after the run completes.

The scripts whose name ends with :code:`_loop` additionally include logic to relaunch
a failed job up to some fixed number of times within the allocation.
Between launches, the scripts call :code:`scr_list_down_nodes` to detect and avoid failed nodes for the relaunch.
One may allocate spare nodes when using these scripts.

These job scripts serve as templates that one can modify as needed.
However, they can often be used as drop in replacements for the launch command in an existing job script.

In addition to the example bash job scripts, an :code:`scr_run` command is in the :code:`/bin` directory of an SCR installation.
This is a python script that, like the bash scripts, executes :code:`scr_prerun`, :code:`scr_postrun`,
and it optionally relaunches a run after detecting and excluding any failed nodes.
When using :code:`scr_run`, one must specify the job launcher as the first argument::

  SLURM: scr_run srun [srun_options]  <prog> [prog_args ...]
  LSF:   scr_run jsrun [jsrun_options]  <prog> [prog_args ...]

Using the SCR wrapper scripts
-----------------------------
All wrapper scripts must run from within a job allocation.
The commands must know the SCR prefix directory.
By default, this is assumed to be the current working directory.
For the :code:`scr_run` script,
one may specify a different prefix directory by setting the :code:`SCR_PREFIX` parameter.

When using any of these scripts, it is recommended to set the :code:`SCR_HALT_SECONDS`
parameter so that the job allocation does not expire before
datasets can be flushed (Section :ref:`sec-halt`).

By default, the :code:`scr_run` script does not restart an application after the first job step exits.
To automatically restart a job step within the current allocation,
set the :code:`SCR_RUNS` environment variable to the maximum number of runs to attempt.
For an unlimited number of attempts, set this variable to :code:`-1`.

After a job step exits, the wrapper script checks whether it should restart the job.
If so, the script sleeps for some time to give nodes in the allocation a chance to clean up.
Then it checks that there are sufficient healthy nodes remaining in the allocation.
By default, the script assumes the next run requires the same number of nodes as the previous run,
which is recorded in a file written by the SCR library.
If this file cannot be read, the command assumes the application requires all nodes in the allocation.
Alternatively, one may override these heuristics and precisely specify the number of nodes needed
by setting the :code:`SCR_MIN_NODES` environment variable to the number of required nodes.

See Section :ref:`sec-config` for additional common SCR configuration settings.

For applications that cannot invoke the SCR wrapper scripts as described here,
one should examine the logic contained within the script and duplicate the necessary parts
in the job batch script.
In particular, one should invoke :code:`scr_postrun` for scavenge support.

Example batch script for using scavenge, but no restart
-------------------------------------------------------

An example SLURM batch script with :code:`scr_srun.sh` is shown below

.. code-block:: bash

  #!/bin/bash
  #SBATCH --no-kill

  # halt if there is less than an hour remaining (3600 seconds)
  export SCR_HALT_SECONDS=3600

  # run the job with scr_srun
  scr_run.sh -n512 -N64 ./my_job

Example batch script for using scavenge and restart
---------------------------------------------------

An example SLURM batch script with :code:`scr_srun_loop.sh` is shown below

.. code-block:: bash

  #!/bin/bash
  #SBATCH --no-kill

  # halt if there is less than an hour remaining (3600 seconds)
  export SCR_HALT_SECONDS=3600

  # run the job with scr_srun, will run up to 5 times
  scr_run_loop.sh -n512 -N64 ./my_job

Example SLURM batch script with :code:`scr_run` using scavenge and restart
--------------------------------------------------------------------------

.. code-block:: bash

  #!/bin/bash
  #SBATCH --no-kill
  #SBATCH --nodes 66

  # above, tell SLURM to not kill the job allocation upon a node failure
  # also note that the job requested 2 spares -- it uses 64 nodes but allocated 66

  # specify where datasets should be written
  export SCR_PREFIX=/parallel/file/system/username/run1

  # instruct SCR to flush to the file system every 20 checkpoints
  export SCR_FLUSH=20

  # halt if there is less than an hour remaining (3600 seconds)
  export SCR_HALT_SECONDS=3600

  # attempt to run the job up to 3 times
  export SCR_RUNS=3

  # run the job with scr_srun
  scr_run srun -n512 -N64 ./my_job


Managing SCR jobs with Python scripts
--------------------------------------

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

ClusterShell (optional)
^^^^^^^^^^^^^^^^^^^^^^^

The ``scrjob`` package can use the ClusterShell module if available.
This can be enabled by setting ``USE_CLUSTERSHELL = True`` in ``scrjob/config.py``.
ClusterShell is not used if it is not found or if it is disabled.

ClusterShell is useful as:

- an ``scr_hostlist`` replacement for manipulating host lists
- a ``pdsh`` replacement for running commands on a set of compute nodes

**Installation**

.. code-block:: bash

  pip install ClusterShell


To configure ClusterShell, see:

- `ClusterShell config docs`_
- ``man clush.conf``

Node groups can be bound together by differing lists. 
Library defaults may need to be overridden for identifying nodes
(see bottom of the config.html link) 
The ClusterShell ``NodeSet`` class supports more operations than ``scr_hostlist``.
See the NodeSet class: ``ClusterShell.NodeSet.NodeSet`` (`ClusterShell Usage`_
).

.. _`ClusterShell config docs`: https://clustershell.readthedocs.io/en/latest/config.html
.. _`ClusterShell Usage`: https://clustershell.readthedocs.io/en/latest/guide/taskmgnt.html


SCR User Commands
^^^^^^^^^^^^^^^^^

Along with other binaries, these scripts are copied to the ``/bin`` directory of an SCR installation.
The scripts are installed with their executable bit set, and the ``.py`` suffix is dropped.
These are user commands that are typically invoked interactively or from a batch job script.
Detailed usage for the scripts in this directory is provided in the SCR user documentation.

**SCR commands**

- ``scr_prerun``          - Execute before the first SCR job in an allocation
- ``scr_postrun``         - Execute after the final SCR job in an allocation; scavenges any cached datasets
- ``scr_list_down_nodes`` - Reports list of currently failed nodes in an allocation, if any
- ``scr_should_exit``     - Indicates whether one should stop launching SCR runs within an allocation; checks for active halt condition, insufficient nodes, or in sufficient time
- ``scr_halt``            - View/edit/remove conditions in the halt file  

**scr\_run**

The ``scr_run`` script provides a high-level wrapper around the above scipts.
It can automatically relaunch a job and avoid down nodes after detecting a failure,
and it scavenges any cached datasets before exiting the allocation.
  
**Usage:**
 
Typical:

.. code-block:: bash
  
  scr_run <launcher> <launcher args> <program> <program args>

 
Extended options:

.. code-block:: bash
  
  scr_run <launcher> [-rc|--run-cmd]=<run command> [-rs|--restart-cmd]=<restart command> <launcher args>

Using extended options the commands will be modified as follows:  

If not restarting or ``scr_have_restart`` returns ``None``: 
``<launcher> <launcher args> <run command>``  

If restarting and the most recent checkpoint name is identified,  
instances of `SCR_CKPT_NAME` will be replaced with the checkpoint name in the restart command:  
``<launcher> <launcher args> <restart command>``  
