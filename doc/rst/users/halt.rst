.. highlight:: bash

.. _sec-halt:

Halt a job
==========

When SCR is configured to write datasets to cache,
one needs to take care when terminating a job early
so that SCR can copy datasets from cache to the
parallel file system before the job allocation ends.
This section describes methods to cleanly halt a job,
including detection and termination of a hanging job.

scr_halt and the halt file
--------------------------

The recommended method to stop an SCR application is to use the :code:`scr_halt` command.
The command must be run from within the prefix directory,
or otherwise, the prefix directory of the target job must be specified as an argument.

A number of different halt conditions can be specified.
In most cases, the :code:`scr_halt` command communicates these conditions to the running
application via the :code:`halt.scr` file,
which is stored in the hidden :code:`.scr` directory within the prefix directory.
The application can determine when to exit by calling :code:`SCR_Should_exit`.

Additionally, one can set :code:`SCR_HALT_EXIT=1` to configure SCR to exit the job
if it detects an active halt condition.
In that case, the SCR library reads the halt file when the application calls :code:`SCR_Init`
and during :code:`SCR_Complete_output` after each complete checkpoint.
If a halt condition is satisfied, all tasks in the application call :code:`exit`.

Halt after next checkpoint
--------------------------

You can instruct an SCR job to halt after completing its next successful checkpoint::

  scr_halt

To run :code:`scr_halt` from outside of a prefix directory,
specify the target prefix directory like so::

  scr_halt /p/lscratcha/user1/simulation123

You can instruct an SCR job to halt after completing some number of checkpoints
via the :code:`--checkpoints` option.
For example, to instruct a job to halt after 10 more checkpoints, use the following::

  scr_halt --checkpoints 10

If the last of the checkpoints is unsuccessful,
the job continues until it completes a successful checkpoint.
This ensures that SCR has a successful checkpoint to flush before it halts the job.

Halt before or after a specified time
-------------------------------------

It is possible to instruct an SCR job to halt *after* a specified time using
the :code:`--after` option.
The job will halt on its first successful checkpoint after the specified time.
The time must be specified in ISO format, for example::

  scr_halt --after '2023-07-04T21:00:00'

It is also possible to instruct a job to halt *before* a specified time
using the :code:`--before` option, for example::

  scr_halt --before '2023-07-04T21:00:00'

For the "halt before" condition to be effective,
one must also set the :code:`SCR_HALT_SECONDS` parameter.
When :code:`SCR_HALT_SECONDS` is set to a positive number,
SCR checks how much time is left before the specified time limit.
If the remaining time in seconds is less than or equal to :code:`SCR_HALT_SECONDS`, SCR halts the job.
The value of :code:`SCR_HALT_SECONDS` does not affect the "halt after" condition.

It is highly recommended that :code:`SCR_HALT_SECONDS` be set
so that the SCR library can impose a default "halt before" condition using the end time
of the job allocation.
This ensures the latest checkpoint can be flushed before the allocation is lost.

It is important to set :code:`SCR_HALT_SECONDS` to a value large enough
that SCR has time to completely flush (and rebuild) files before the allocation expires.
Consider that a checkpoint may be taken just *before* the
remaining time is less than :code:`SCR_HALT_SECONDS`.
If a code checkpoints every X seconds and it takes Y seconds
to flush files from the cache and rebuild, set :code:`SCR_HALT_SECONDS` = X + Y + Delta,
where Delta is some positive value to provide additional slack time.

One may also set the halt seconds via the :code:`--seconds` option to :code:`scr_halt`.
Using the :code:`scr_halt` command, one can set, change, and unset the halt seconds on a running job.

NOTE: If any :code:`scr_halt` commands are specified as part of the batch script before
the first run starts,
one must then use :code:`scr_halt` to set the halt seconds for the job rather than
the :code:`SCR_HALT_SECONDS` parameter.
The :code:`scr_halt` command creates the halt file,
and if a halt file exists before a job starts to run,
SCR ignores any value specified in the :code:`SCR_HALT_SECONDS` parameter.

Halt immediately
----------------

Sometimes, you need to halt an SCR job immediately, and there are two options for this.
You may use the :code:`--immediate` option::

  scr_halt --immediate

This command first updates the halt file, so that the job will not be restarted once stopped.
Then, it kills the current run.

If for some reason the :code:`--immediate` option fails to work,
you may manually halt the job. [#fcray]_
First, issue a simple :code:`scr_halt` so the job will not restart,
and then manually kill the current run using mechanisms provided by the resource manager,
e.g., :code:`scancel` for SLURM and :code:`apkill` for ALPS.
When using mechanisms provided by the resource manager to kill the
current run, be careful to cancel the job step and not the job allocation.
Canceling the job allocation destroys the cache.

For SLURM, to get the job step id, type: :code:`squeue -s`.
Then be sure to include the job id *and* step id in the :code:`scancel` argument.
For example, if the job id is 1234 and the step id is 5, then use the following commands::

  scr_halt
  scancel 1234.5

Do *not* just type :code:`scancel 1234` -- be sure to include the job step id.

For ALPS, use :code:`apstat` to get the apid of the job step to kill.
Then, follow the steps as described above: execute :code:`scr_halt`
followed by the kill command :code:`apkill <apid>`.

.. [#fcray] On Cray/ALPS, :code:`scr_halt --immediate` is not yet supported. The alternate method described in the text must be used instead.

.. _sec-hang:

Catch a hanging job
-------------------

If an application hangs, SCR may not be given the chance
to copy files from cache to the parallel file system before the allocation expires.
To avoid losing significant work due to a hang,
SCR attempts to detect if a job is hanging, and if so, 
SCR attempts to kill the job step so that it can be restarted in the allocation.

On some systems, SCR employs the :code:`io-watchdog`
library for this purpose. 
For more information on this tool, see http://code.google.com/p/io-watchdog.

On systems where :code:`io-watchdog` is not available, 
SCR uses a generic mechanism based on the expected
time between checkpoints as specified by the user. If the time between checkpoints 
is longer than expected, SCR assumes the job is hanging.
Two SCR parameters determine how many seconds should pass
between I/O phases in an application, i.e. seconds between
consecutive calls to :code:`SCR_Start_output`.
These are :code:`SCR_WATCHDOG_TIMEOUT`
and :code:`SCR_WATCHDOG_TIMEOUT_PFS`. The first parameter
specifies the time to wait when SCR writes checkpoints to
in-system storage, e.g. SSD or RAM disk, and the second
parameter specifies the time to wait when SCR writes
checkpoints to the parallel file system. 
The reason for the two timeouts is that writing to the parallel
file system generally takes much longer than writing to in-system
storage, and so a longer timeout period is useful in that case.

When using this feature, be careful to check that the job does not hang near the end of its allocation time limit,
since in this case, SCR may not kill the run with enough time before the allocation ends.
If you suspect the job to be hanging and you deem that SCR will not
kill the run in sufficient time, manually cancel the run as described above.

Combine, list, change, and unset halt conditions
------------------------------------------------

It is possible to specify multiple halt conditions.
To do so, simply list each condition in the same :code:`scr_halt` command or issue several commands.
For example, to instruct a job to halt after 10 checkpoints or before a certain time,
which ever comes earlier, you could issue the following command::

  scr_halt --checkpoints 10 --before '2023-07-04T21:00:00'

The following sequence also works::

  scr_halt --checkpoints 10
  scr_halt --before '2023-07-04T21:00:00'

You may list the current settings in the halt file with the :code:`--list` option, e.g.,::

  scr_halt --list

You may change a setting by issuing a new command to overwrite the current value.

Finally, you can unset some halt conditions by prepending :code:`unset-` to the option names.
See the :code:`scr_halt` man page for a full listing of unset options.
For example, to unset the "halt before" condition on a job, type the following::

  scr_halt --unset-before

Remove the halt file
--------------------

Sometimes, especially during testing, you may want to run in an existing
allocation after halting a previous run.
When SCR detects a halt file with a satisfied halt condition, it immediately exits.
This is the desired effect when trying to halt a job,
however this mechanism also prevents one from intentionally running in an allocation
after halting a previous run.
Along these lines, know that SCR registers a halt condition whenever
the application calls :code:`SCR_Finalize`.

When there is a halt file with a satisfied halt condition,
a message is printed to :code:`stdout` to indicate why SCR is halting.
To run in such a case, first remove the satisfied halt conditions.
You can unset the conditions or reset them to appropriate values.
Another approach is to remove the halt file via the :code:`--remove` option.
This deletes the halt file, which effectively removes all halt conditions.
For example, to remove the halt file from a job, type::

  scr_halt --remove

