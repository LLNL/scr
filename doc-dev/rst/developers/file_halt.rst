.. _halt_file:

Halt file
---------

The halt file tracks various conditions that are used to determine
whether or not a run should continue to execute. The halt file is kept
in the prefix directory. It is updated by the library during the run,
and it is also updated externally through the ``scr_halt`` command.
Internally, the data of the halt file is organized as a hash. Here are
the contents of an example halt file.

::

     CheckpointsLeft
       7
     ExitAfter
       1298937600
     ExitBefore
       1298944800
     HaltSeconds
       1200
     ExitReason
       SCR_FINALIZE_CALLED

The ``CheckpointsLeft`` field provides a counter on the number of
checkpoints that should be completed before SCR stops the job. With each
checkpoint, the library decrements this counter, and the run stops if it
hits 0.

The ``ExitAfter`` field records a timestamp (seconds since UNIX epoch).
At various times, SCR compares the current time to this timestamp, and
it halts the run as soon as the current time exceeds this timestamp.

The ``ExitBefore`` field combined with the ``HaltSeconds`` field inform
SCR that the run should be halted at specified number of seconds before
a specified time. Again, SCR compares the current time to the time
specified by subtracting the ``HaltSeconds`` value from the
``ExitBefore`` timestamp (seconds since UNIX epoch). If the current time
is equal to or greater than this time, SCR halts the run.

Finally, the ``ExitReason`` field records a reason the job is or should
be halted. If SCR ever detects that this field is set, it halts the job.

A user can add, modify, and remove halt conditions on a running job
using the ``scr_halt`` command. Each time an application completes a
dataset, SCR checks settings in the halt file. If any halt condition is
satisfied, SCR flushes the most recent checkpoint, and then each process
calls ``exit()``. Control is not returned to the application.
