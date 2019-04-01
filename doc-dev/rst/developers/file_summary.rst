.. _summary_file:

Summary file
------------

The summary file tracks global properties of a dataset, such as dataset
id, its size, the total number of files, and the time it was created. It
is stored in the dataset directory on the parallel file system.
Internally, the data of the summary file is organized as a hash. Here
are the contents of an example summary file.

::

     VERSION
       6
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

The ``VERSION`` field records the version number of file format of the
summary file. This enables future SCR implementations to change the
format of the summary file while still allowing SCR to read summary
files written by older implementations.

A ``COMPLETE`` flag concisely denotes whether all files for this dataset
are thought to be valid. The properties of the dataset are then
contained within the ``DSET`` hash.

When fetching a checkpoint upon a restart, rank 0 reads the summary file
and broadcasts its contents to the other ranks.
