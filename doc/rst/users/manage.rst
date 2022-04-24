.. highlight:: bash

.. _sec-scr_index:

Manage datasets
===============

SCR records the status of datasets that are on the parallel file system in the :code:`index.scr` file.
This file is written to the hidden :code:`.scr` directory within the prefix directory.
The library updates the index file as an application runs and during scavenge operations.

While restarting a job, the SCR library reads the index file during :code:`SCR_Init`
to determine which checkpoints are available.
The library attempts to restart with the most recent checkpoint and works backwards
until it successfully loads a valid checkpoint.
SCR does not restart from any checkpoint marked as "incomplete" or "failed".
A checkpoint is marked as incomplete if it was determined to be invalid during the flush or scavenge.
Additionally, the library marks a checkpoint as failed if it detected a problem
during a previous restart attempt (e.g., detected data corruption).
In this way, the library avoids invalid or problematic checkpoints.

One may list or modify the contents of the index file via the :code:`scr_index` command.
The :code:`scr_index` command must run within the prefix directory,
or otherwise, one may specify a prefix directory using the :code:`--prefix` option.
The default behavior of :code:`scr_index` is to list the contents of the index file, e.g.::

  >>: scr_index
  DSET VALID FLUSHED             CUR NAME
    18 YES   2014-01-14T11:26:06   * ckpt.18
    12 YES   2014-01-14T10:28:23     ckpt.12
     6 YES   2014-01-14T09:27:15     ckpt.6

When listing datasets, :code:`scr_index` lists a field indicating whether the dataset is valid,
the time it was flushed to the parallel file system,
and finally the dataset name.

One checkpoint may also be marked as "current".
When restarting a job, the SCR library starts from the current dataset and works backwards.
The current dataset is denoted with a leading :code:`*` character in the :code:`CUR` column.
One can change the current checkpoint using the :code:`--current` option,
providing the dataset name as an argument::

  scr_index --current ckpt.12

If no dataset is marked as current,
SCR starts with most recent valid checkpoint.

One may drop entries from the index file using the :code:`--drop` option.
This operation does not delete the corresponding dataset files.
It only drops the entry from the :code:`index.scr` file::

  scr_index --drop ckpt.50

This is useful if one deletes a dataset from the parallel file system
and then wishes to update the index.

If an entry is removed inadvertently, one may add it back with::

  scr_index --add ckpt.50

This requires all SCR metadata files to exist in their associated :code:`scr.dataset` subdirectory
within the hidden :code:`.scr` directory within the prefix directory.

In most cases, the SCR library or the SCR commands add all necessary entries to the index file.
However, there are cases where they may fail.
In particular, if the :code:`scr_postrun` command successfully scavenges a dataset
but the resource allocation ends before the command can rebuild missing files,
an entry may be missing from the index file.
In such cases, one may manually add the corresponding entry
using the :code:`--build` option.

When adding a new dataset to the index file,
the :code:`scr_index` command checks whether the files in a dataset
constitute a complete and valid set.
It rebuilds missing files if there are sufficient redundant data,
and it writes the :code:`summary.scr` file for the dataset if needed.
One must provide the SCR dataset id as an argument.
To obtain the SCR dataset id value, lookup the trailing integer on the names of :code:`scr.dataset` subdirectories
in the hidden :code:`.scr` directory within the prefix directory::

  scr_index --build 50

