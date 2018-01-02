.. _sec-scripts:

Run a job
=========

In addition to the SCR library,
one must properly configure the batch system
and include a set of SCR commands in the job batch script.
In particular, one must:
1) inform the batch system that the allocation should remain available even after a failure
and 2) replace the command to execute the application with an SCR wrapper script.
The precise set of options and commands to use depends on the system resource manager.

The SCR commands prepare the cache, scavenge files from cache to the parallel file system,
and check that the scavenged dataset is complete among other things.
These commands are located in the :code:`/bin` directory where SCR is installed.
There are numerous SCR commands.
Any command not mentioned in this document is
not intended to be executed by users.

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
the principles apply generally across SCR-supported resource managers.

When a job is scheduled resources on a system,
the batch script executes inside of a job allocation.
The job allocation consists of a set of nodes, a time limit, and a job id.
The job id can be obtained by executing the :code:`squeue` command
on SLURM, the :code:`apstat` command on ALPS, and the :code:`bjobs` command on LSF.

Within a job allocation, a user may run one or more job steps,
each of which is invoked by a call to :code:`srun` on SLURM, :code:`aprun` on ALPS, or :code:`mpirun` on LSF.
Each job step is assigned its own step id.
On SLURM, within each job allocation, job step ids start at 0 and increment with each issued job step.
Job step ids can be obtained by passing the :code:`-s` option to :code:`squeue`.
A fully qualified name of a SLURM job step consists of: :code:`jobid.stepid`.
For instance, the name :code:`1234.5` refers to step id 5 of job id 1234.
On ALPS, each job step within an allocation has a unique id that can be obtained
through :code:`apstat`.

Ignoring node failures
----------------------

Before running an SCR job, it is necessary to configure the job allocation to withstand node failures.
By default, most resource managers terminate the job allocation if a node fails,
however SCR requires the job allocation to remain active in order to restart the job or to scavenge files.
To enable the job allocation to continue past node failures,
one must specify the appropriate flags from the table below.

SCR job allocation flags

================== ================================================================
MOAB batch script  :code:`#MSUB -l resfailpolicy=ignore`
MOAB interactive   :code:`qsub -I ... -l resfailpolicy=ignore`
SLURM batch script :code:`#SBATCH --no-kill`
SLURM interactive  :code:`salloc --no-kill ...`
LSF batch script   :code:`#BSUB -env "all, LSB_DJOB_COMMFAIL_ACTION=KILL_TASKS"`
LSF interactive    :code:`bsub -env "all, LSB_DJOB_COMMFAIL_ACTION=KILL_TASKS" ...`
================== ================================================================

The SCR wrapper script
----------------------
The easiest way to integrate SCR into a batch script is to set some environment variables
and to replace the job run command with an SCR wrapper script.
The SCR wrapper script includes logic to restart an application within an job allocation,
and it scavenges files from cache to the parallel file system at the end of an allocation.::

  SLURM:  scr_srun [srun_options]  <prog> [prog_args ...]
  ALPS:   scr_aprun [aprun_options] <prog> [prog_args ...]
  LSF:    scr_mpirun [mpirun_options] <prog> [prog_args ...]

The SCR wrapper script must run from within a job allocation.
Internally, the command must know the prefix directory.
By default, it uses the current working directory.
One may specify a different prefix directory by setting the :code:`SCR_PREFIX` parameter.

It is recommended to set the :code:`SCR_HALT_SECONDS`
parameter so that the job allocation does not expire before
datasets can be flushed (Section :ref:`sec-halt`).


By default, the SCR wrapper script does not restart an application after the first job step exits.
To automatically restart a job step within the current allocation,
set the :code:`SCR_RUNS` environment variable to the maximum number of runs to attempt.
For an unlimited number of attempts, set this variable to :code:`-1`.

After a job step exits, the wrapper script checks whether it should restart the job.
If so, the script sleeps for some time to give nodes in the allocation a chance to clean up.
Then, it checks that there are sufficient healthy nodes remaining in the allocation.
By default, the wrapper script assumes the next run requires the same number of nodes as the previous run,
which is recorded in a file written by the SCR library.
If this file cannot be read, the command assumes the application requires all nodes in the allocation.
Alternatively, one may override these heuristics and precisely specify the number of nodes needed
by setting the :code:`SCR_MIN_NODES` environment variable to the number of required nodes.

Some applications cannot run via wrapper scripts.
For applications that cannot invoke the SCR wrapper script as described here,
one should examine the logic contained in the script and duplicate the necessary parts
in the job batch script.
In particular, one should invoke :code:`scr_postrun` for scavenge support.

Example batch script for using SCR restart capability
-----------------------------------------------------

An example MOAB / SLURM batch script with :code:`scr_srun` is shown below

.. code-block:: bash

  #!/bin/bash
  #MSUB -l partition=atlas
  #MSUB -l nodes=66
  #MSUB -l resfailpolicy=ignore
  
  # above, tell MOAB to not kill the job allocation upon a node failure
  # also note that the job requested 2 spares -- it uses 64 nodes but allocated 66
  
  # specify where datasets should be written
  export SCR_PREFIX=/my/parallel/file/system/username/run1/checkpoints
  
  # instruct SCR to flush to the file system every 20 checkpoints
  export SCR_FLUSH=20
  
  # halt if there is less than an hour remaining (3600 seconds)
  export SCR_HALT_SECONDS=3600
  
  # attempt to run the job up to 3 times
  export SCR_RUNS=3
  
  # run the job with scr_srun
  scr_srun -n512 -N64 ./my_job
