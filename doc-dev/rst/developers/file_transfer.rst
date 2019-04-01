.. _transfer_file:

Transfer file
-------------

When using the asynchronous flush, the library creates a dataset
directory within the prefix directory, and then it relies on an external
task to actually copy data from the cache to the parallel file system.
The library communicates when and what files should be copied by
updating the transfer file. A ``scr_transfer`` daemon process running in
the background on each compute node periodically reads this file to
check whether any files needs to be copied. If so, it copies data out in
small bursts, sleeping a short time between bursts in order to throttle
its CPU and bandwidth usage. The code for this daemon is in
``scr_transfer.c``. Here is what the contents of a transfer file look
like:

::

     FILES
       /tmp/user1/scr.1001186/index.0/dataset.1/rank_0.ckpt
         DESTINATION
           /p/lscratchb/user1/simulation123/scr.dataset.1/rank_0.ckpt 
         SIZE
           524294
         WRITTEN
           524294
       /tmp/user1/scr.1001186/index.0/dataset.1/rank_0.ckpt.scr
         DESTINATION
           /p/lscratchb/user1/simulation123/scr.dataset.1/rank_0.ckpt.scr 
         SIZE
           124
         WRITTEN
           124
     PERCENT
       0.000000
     BW
       52428800.000000
     COMMAND
       RUN
     STATE
       STOPPED
     FLAG
       DONE

The library specifies the list of files to be flushed by absolute file
name under the ``FILES`` hash. For each file, the library specifies the
size of the file (in bytes) under ``SIZE``, and it specifies the
absolute path where the file should be written to under ``DESTINATION``.

The library also specifies limits for the ``scr_transfer`` process. The
``PERCENT`` field specifies the percentage of CPU time the
``scr_transfer`` process should spend running. The daemon monitors how
long it runs for when issuing a write burst, and then it sleeps for an
appropriate amount of time before executing the next write burst so that
it stays below this threshold. The ``BW`` field specifies the amount of
bandwidth the daemon may consume (in bytes/sec) while copying data. The
daemon process monitors how much data it has written along with the time
taken to write that data, and it adjusts its sleep periods between write
bursts to keep below its bandwidth limit.

Once the library has specified the list of files to be transferred and
set any limits for the ``scr_transfer`` process, it sets the ``COMMAND``
field to ``RUN``. The ``scr_transfer`` process does not start to copy
data until this ``RUN`` command is issued. The library may also specify
the ``EXIT`` command, which causes the ``scr_transfer`` process to exit.

The ``scr_transfer`` process records its current state in the ``STATE``
field, which may be one of: ``STOPPED`` (waiting to do something) and
``RUNNING`` (actively flushing). As the ``scr_transfer`` process copies
each file out, it records the number of bytes it has written (and
fsync’d) under the ``WRITTEN`` field. When all files in the list have
been copied, ``scr_transfer`` sets the ``DONE`` flag under the ``FLAG``
field. The library periodically looks for this flag, and once set, the
library completes the flush by writing the summary file in the dataset
directory and updating the index file in the prefix directory.
