.. _index_file:

Index file
----------

The index file records information about each of the datasets stored in
the prefix directory on the parallel file system. It is stored in the
prefix directory. Internally, the data of the index file is organized as
a hash. Here are the contents of an example index file.

::

     VERSION
       1
     CURRENT
       scr.dataset.18
     DIR
       scr.dataset.18
         DSET
           18
       scr.dataset.12
         DSET
           12
     DSET
       18
         DIR
           scr.dataset.18
             COMPLETE
               1
             DSET
               ID
                 18
               NAME
                 scr.dataset.18
               CREATED
                 1312853507675143
               USER
                 user1
               JOBNAME
                 simulation123
               JOBID
                 112573
               CKPT
                 18
               FILES
                 4
               SIZE
                 2097182
               COMPLETE
                 1
             FLUSHED
               2011-08-08T18:31:47
       12
         DIR
           scr.dataset.12
             FETCHED
               2011-08-08T18:31:47
             FLUSHED
               2011-08-08T18:30:30
             COMPLETE
               1
             DSET
               COMPLETE
                 1
               SIZE
                 2097182
               FILES
                 4
               ID
                 12
               NAME
                 scr.dataset.12
               CREATED
                 1312853406814268
               USER
                 user1
               JOBNAME
                 simulation123
               JOBID
                 112573
               CKPT
                 12

The ``VERSION`` field records the version number of file format of the
index file. This enables future SCR implementations to change the format
of the index file while still allowing SCR to read index files written
by older implementations.

The ``CURRENT`` field specifies the name of a dataset directory. When
restarting a job, SCR starts with this directory. It then works
backwards from this directory, searching for the most recent checkpoint
(the checkpoint having the highest id) that is thought to be complete
and that has not failed a previous fetch attempt.

The ``DIR`` hash is a simple index which maps a directory name to a
dataset id.

The information for each dataset is indexed by dataset id under the
``DSET`` hash. There may be multiple copies of a given dataset id, each
stored within a different dataset directory within the prefix directory.
For a given dataset id, each copy is indexed by directory name under the
``DIR`` hash. For each directory, SCR tracks whether the set of dataset
files is thought to be complete (``COMPLETE``), the timestamp at which
the dataset was copied to the parallel file system (``FLUSHED``),
timestamps at which the dataset (checkpoint) was fetched to restart a
job (``FETCHED``), and timestamps at which a fetch attempt of this
dataset (checkpoint) failed (``FAILED``).
