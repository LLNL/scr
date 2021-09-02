.. _sec-assumptions:

Assumptions
===========

A number of assumptions are made in the SCR implementation.
If any of these assumptions do not hold for an application,
the application may not be able to use certain features of SCR,
or it might not be able to use SCR at all.
If this is the case, or if you have any questions, please notify the SCR developers.
The goal is to expand the implementation to support a large number of applications.

* The code must be an MPI application.
* For best performance, the code must read and write datasets
  as a file-per-process in a globally-coordinated fashion.
  There is support for reading and writing shared files,
  but one cannot utilize the fastest storage methods in that case.
  See Section :ref:`sec-config-common` for details.
* To use the scalable restart capability,
  a job must be restarted with the same number of processes it ran with when it wrote the checkpoint,
  and each process must only access the files it wrote during the checkpoint.
  Note that this may limit the effectiveness of the library for codes that can restart
  from a checkpoint with a different number of processes than were used to write the checkpoint.
  Such codes may still benefit from the scalable checkpoint capability,
  but they must configure SCR to restart from the parallel file system.
* It must be possible to store the dataset files from all processes in the same directory.
  In particular, all files belonging to a given dataset must have distinct names.
* Files cannot contain data that span multiple datasets.
  In particular, there is no support for appending data of the
  current dataset to a file containing data from a previous dataset.
  Each dataset must be self-contained.
* SCR maintains a set of meta data files that it stores in a subdirectory of the directory
  containing the application dataset files.
  The application must allow for these SCR meta data files to coexist with its own files.
* All files must reside under a top-level directory on the parallel file system
  called the "prefix" directory that is specified by the application.
  Under that prefix directory, the application may use subdirectory trees.
  See Section :ref:`sec-checkpoint_directories` for details.
* Applications may configure SCR to cache datasets in RAM disk.
  One must ensure there is sufficient memory capacity to store the dataset files
  after accounting for the memory consumed by the application.
  The amount of storage needed depends on the number of cached datasets
  and the redundancy scheme that is applied.
  See Section :ref:`sec-redundancy` for details.
* Time limits should be imposed so that the SCR library has sufficient time
  to flush files from cache to the parallel file system before the resource allocation expires.
  See Section :ref:`sec-halt` for details.
