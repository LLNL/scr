.. _sec-assumptions:

Assumptions
===========

A number of assumptions are made in the SCR implementation.
If any of these assumptions do not hold for a particular application, that application cannot use SCR.
If this is the case, or if you have any questions, please notify the SCR developers.
The goal is to expand the implementation to support a large number of applications.

* The code must be an MPI application.
* The code must read and write datasets as a file per process in a globally-coordinated fashion.
* A process having a particular MPI rank is only guaranteed access to its own dataset files,
  i.e., a process of a given MPI rank may not access dataset files
  written by a process having a different MPI rank within the same run or across different runs.
* To use the scalable restart capability,
  a job must be restarted with the same number of processes as used to write the checkpoint,
  and each process must only access the files it wrote during the checkpoint.
  Note that this may limit the effectiveness of the library for codes that are capable of restarting
  from a checkpoint with a different number of processes than were used to write the checkpoint.
  Such codes can often still benefit from the scalable checkpoint capability,
  but not the scalable restart -- they must fall back to restarting from the parallel file system.
* It must be possible to store the dataset files from all processes in the same directory.
  In particular, all files belonging to a given dataset must have distinct names.
* SCR maintains a set of meta data files, which it stores in a subdirectory of the directory
  that contains the application dataset files.
  The application must allow for these SCR meta data files to coexist with its own files.
* Files cannot contain data that span multiple datasets.
  In particular, there is no support for appending data of the
  current dataset to a file containing data from a previous dataset.
  Each dataset must be self-contained.
* On some systems, datasets are cached in RAM disk.
  This restricts usage of SCR on those machines to applications whose memory
  footprint leaves sufficient room to store the dataset files in memory
  simultaneously with the running application.
  The amount of storage needed depends on the number of cached datasets
  and the redundancy scheme used.
  See Section :ref:`sec-redundancy` for details.
* SCR occasionally flushes files from cache to the parallel file system.
  All files must reside under a top-level directory on the parallel file system
  called the "prefix" directory that is specified by the application.
  Under that prefix directory, the application may use file and subdirectory trees.
  One constraint is that no two datasets can write to the same file.
  See Section :ref:`sec-checkpoint_directories` for details.
* Time limits should be imposed so that the SCR library has sufficient time
  to flush files from cache to the parallel file system before the resource allocation expires.
  Additionally, care should be taken so that the run does not stop in the middle of a checkpoint.
  See Section :ref:`sec-halt` for details.
