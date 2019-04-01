.. _filemap:

Filemap
=======

Overview
--------

The ``scr_filemap`` data structure maintains the mapping between files,
process ranks, and datasets (checkpoints). In a given dataset, each
process may write zero or more files. SCR uses the filemap to record
which rank writes which file in which dataset. The complete mapping is
distributed among processes. Each process only knows a partial mapping.
A process typically knows the mapping for its own files as well as the
mapping for a few other processes that it provides redundancy data for.

The filemap tracks all files currently in cache, and it is recorded in a
file in the control directory. Each process manages its own *filemap
file*, so that a process may modify its filemap file without
coordinating with other processes. In addition, the master process on
each node maintains a *master filemap file*, which is written to a
well-known name and records the file names of all of the per-process
filemap files that are on the same node.

Before any file is written to the cache, a process adds an entry for the
file to its filemap and then updates its filemap file on disk.
Similarly, after a file is deleted from cache, the corresponding entry
is removed from the filemap and the filemap file is updated on disk.
Following this protocol, a file will not exist in cache unless it has a
corresponding entry in the filemap file. On the other hand, an entry in
the filemap file does not ensure that a corresponding file exists in
cache – it only implies that the corresponding file *may* exist.

When an SCR job starts, the SCR library attempts to read the filemap
files from the control directory to determine what datasets are stored
in cache. The library uses this information to determine which datasets
and which ranks it has data for. The library also uses this data to know
which files to remove when deleting a dataset from cache, and it uses
this data to know which files to copy when flushing a dataset to the
parallel file system.

SCR internally numbers each checkpoint with two unique numbers: a
*dataset id* and a *checkpoint id*. Functions that return dataset or
checkpoint ids return -1 if there is no valid dataset or checkpoint
contained in the filemap that matches a particular query.

The ``scr_filemap`` makes heavy use of the ``scr_hash`` data structure
(Section :ref:`Hash <hash>`). The ``scr_hash`` is utilized in
the ``scr_filemap`` API and its implementation.

.. _filemap_example:

Example filemap hash
--------------------

Internally, filemaps are implemented as ``scr_hash`` objects. Here is an
example hash for a filemap object containing information for ranks 20
and 28 and dataset ids 10 and 11.

::

     DSET
       10
         RANK
           20
       11
         RANK
           20
           28
     RANK
       20
         DSET
           10
             FILES
               2
             FILE
               /<path_to_foo_20.ckpt.10>/foo_20.ckpt.10
                 <meta_data_for_foo_20.ckpt.10>
               /<path_to_foo_20.ckpt.10.xor>/foo_20.ckpt.10.xor
                 <meta_data_for_foo_20.ckpt.10.xor>
             REDDESC
               <redundancy descriptor hash>
             DATADESC
               <dataset_descriptor_for_dataset_10>
           11
             FILES
               1
             FILE
               /<path_to_foo_20.ckpt.11>/foo_20.ckpt.11
                 <meta_data_for_foo_20.ckpt.11>
             REDDESC
               <redundancy descriptor hash>
             DATADESC
               <dataset_descriptor_for_dataset_11>
       28
         DSET
           11
             FILES 
               1
             FILE
               /<path_to_foo_28.ckpt.11>/foo_28.ckpt.11
                 <meta_data_for_foo_28.ckpt.11>
             PARTNER
               atlas56
             REDDESC
               <redundancy descriptor hash>

The main data is kept under the ``RANK`` element at the top level. Rank
ids are listed in the hash of the ``RANK`` element. Within each rank id,
dataset ids are listed in the hash of a ``DSET`` element. Finally, each
dataset id contains elements for the expected number of files (under
``FILES``), the file names (under ``FILE``), the redundancy descriptor
hash (under ``REDDESC``, see
Section :ref:`Redundancy descriptors <redundancy_descriptors>`)
that describes the redundancy scheme applied to the dataset files, and a
hash providing details about the dataset (under ``DATADESC``). In
addition, there may be arbitrary tags such as the ``PARTNER`` element.

Note that the full path to each file is recorded, and a meta data hash
is also recorded for each file, which contains attributes specific to
each file.

There is also a ``DSET`` element at the top level, which lists rank ids
by dataset id. This information is used as an index to provide fast
lookups for certain queries, such as to list all dataset ids in the
filemap, to determine whether there are any entries for a given dataset
id, and to lookup all ranks for a given dataset. This index is kept in
sync with the information contained under the ``RANK`` element.

Common functions
----------------

This section describes some of the most common filemap functions. For a
detailed list of all functions, see ``scr_filemap.h``. The
implementation can be found in ``scr_filemap.c``.

Allocating, freeing, merging, and clearing filemaps
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a new filemap object.

::

     scr_filemap* map = scr_filemap_new();

Free a filemap object.

::

     scr_filemap_delete(&map);

Copy entries from ``filemap_2`` into ``filemap_1``.

::

     scr_filemap_merge(filemap_1, filemap_2);

Delete all entries from a filemap.

::

     scr_filemap_clear(map);

Adding and removing data
~~~~~~~~~~~~~~~~~~~~~~~~

Add an entry for a file for a given rank id and dataset id.

::

     scr_filemap_add_file(map, dset, rank, filename);

Remove an entry for a file for a given rank id and dataset id.

::

     scr_filemap_remove_file(map, dset, rank, filename);

Remove all info corresponding to a given dataset id.

::

     scr_filemap_remove_dataset(map, dset);

Remove all info corresponding to a given rank.

::

     scr_filemap_remove_rank(map, rank);

Remove all info corresponding to a given rank for a given dataset
number.

::

     scr_filemap_remove_rank_by_dataset(map, dset, rank);

Extract all info for a rank from specified map and return as a newly
created filemap. This also deletes the corresponding info from the
source filemap.

::

     scr_filemap* rank_filemap = scr_filemap_extract_rank(map, rank);

Query functions
~~~~~~~~~~~~~~~

Get the number of datasets in a filemap.

::

     int num_dsets = scr_filemap_num_datasets(map);

Get the most recent dataset (highest dataset id).

::

     int dset = scr_filemap_latest_dataset(map);

Get the oldest dataset (lowest dataset id).

::

     int dset = scr_filemap_oldest_dataset(map);

Get the number of ranks in a filemap.

::

     int num_ranks = scr_filemap_num_ranks(map);

Get the number of ranks in a filemap for a given dataset.

::

     int num_ranks = scr_filemap_num_ranks_by_dataset(map, dset);

Determine whether the map contains any data for a specified rank.
Returns 1 if true, 0 if false.

::

     scr_filemap_have_rank(map, rank);

Determine whether the map contains any data for a specified rank for a
given dataset id. Returns 1 if true, 0 if false.

::

     scr_filemap_have_rank_by_dataset(map, dset, rank);

For a given rank in a given dataset, there are two file counts that are
of interest. First, there is the “expected” number of files. This refers
to the number of files that a process wrote during the dataset. Second,
there is the “actual” number of files the filemap contains data for.
This distinction enables SCR to determine whether a filemap contains
data for all files a process wrote during a given dataset.

For a given rank id and dataset id, get the number of files the filemap
contains info for.

::

     int num_files = scr_filemap_num_files(map, dset, rank);

Set the number of expected files for a rank during a given dataset.

::

     scr_filemap_set_expected_files(map, dset, rank, num_expected_files);

Get the number of expected files for a rank during a dataset.

::

     int num_expected_files = scr_filemap_get_expected_files(map, dset, rank);

Unset the number of expected files for a given rank and dataset.

::

     scr_filemap_unset_expected_files(map, dset, rank);

List functions
~~~~~~~~~~~~~~

There a number of functions to return a list of entries in a filemap.
The function will allocate and return the list in an output parameter.
The caller is responsible for freeing the list if it is not NULL.

Get a list of all dataset ids (ordered oldest to most recent).

::

     int ndsets;
     int* dsets;
     scr_filemap_list_datasets(map, &ndsets, &dsets);
     ...
     if (dsets != NULL)
       free(dsets);

Get a list of all rank ids (ordered smallest to largest).

::

     int nranks;
     int* ranks;
     scr_filemap_list_ranks(map, &nranks, &ranks);
     ...
     if (ranks != NULL)
       free(ranks);

Get a list of all rank ids for a given dataset (ordered smallest to
largest).

::

     int nranks;
     int* ranks;
     scr_filemap_list_ranks_by_dataset(map, dset, &nranks, &ranks);
     ...
     if (ranks != NULL)
       free(ranks);

To get a count of files and a list of file names contained in the
filemap for a given rank id in a given dataset. The list is in arbitrary
order.

::

     int nfiles;
     char** files;
     scr_filemap_list_files(map, ckpt, rank, &nfiles, &files);
     ...
     if (files != NULL)
       free(files);

In this last case, the pointers returned in files point to the strings
in the elements within the filemap. Thus, if any elements are deleted or
changed, these pointers will be invalid and should not be dereferenced.
In this case, a new list of files should be obtained.

When using the above functions, the caller is responsible for freeing
memory allocated to store the list if it is not NULL.

Iterator functions
~~~~~~~~~~~~~~~~~~

One may obtain a pointer to an ``scr_hash_elem`` object which can be
used with the ``scr_hash`` functions to iterate through the values of a
filemap. The iteration order is arbitrary.

To iterate through the dataset ids contained in a filemap.

::

     scr_hash_elem* elem = scr_filemap_first_dataset(map);

To iterate through the ranks contained in a filemap for a given dataset
id.

::

     scr_hash_elem* elem = scr_filemap_first_rank_by_dataset(map, dset);

To iterate through the files contained in a filemap for a given rank id
and dataset id.

::

     scr_hash_elem* elem = scr_filemap_first_file(map, dset, rank);

Dataset descriptors
~~~~~~~~~~~~~~~~~~~

The filemap also records dataset descriptors for a given rank and
dataset id. These descriptors associate attributes with a dataset (see
Section :ref:`Datasets <datasets>`).

To record a dataset descriptor for a given rank and dataset id.

::

     scr_filemap_set_dataset(map, dset, rank, desc);

To get a dataset descriptor for a given rank and dataset id.

::

     scr_dataset* desc = scr_dataset_new();
     scr_filemap_get_dataset(map, dset, rank, desc);

To unset a dataset descriptor for a given rank and dataset id.

::

     scr_filemap_unset_dataset(map, dset, rank);

File meta data
~~~~~~~~~~~~~~

In addition to recording the filenames for a given rank and dataset, the
filemap also records meta data for each file, including the expected
size of the file and CRC32 checksums (see
Section \ :ref:`Meta data <meta>`).

To record meta data for a file.

::

     scr_filemap_set_meta(map, dset, rank, file, meta);

To get a meta data for a file.

::

     scr_meta* meta = scr_meta_new();
     scr_filemap_get_meta(map, dset, rank, file, meta);

To unset meta data for a file.

::

     scr_filemap_unset_meta(map, dset, rank, file);

One must specify the same filename that was used during the call to
``scr_filemap_add_file()``.

.. _filemap_redundancy_descriptors:

Redundancy descriptors
~~~~~~~~~~~~~~~~~~~~~~

A redundancy descriptor is a data structure that describes the location
and redundancy scheme that is applied to a set of dataset files in cache
(Section :ref:`Redundancy descriptors <redundancy_descriptors>`).
In addition to knowing what dataset files are in cache, it’s also useful
to know what redundancy scheme is applied to that data. To do this, a
redundancy descriptor can be associated with a given dataset and rank in
the filemap.

Given a redundancy descriptor hash, associate it with a given dataset id
and rank id.

::

     scr_filemap_set_desc(map, dset, rank, desc);

Given a dataset id and rank id, get the corresponding descriptor.

::

     scr_filemap_get_desc(map, dset, rank, desc);

Unset a redundancy descriptor.

::

     scr_filemap_unset_desc(map, ckpt, rank)

Tags
~~~~

One may also associate arbitrary key/value string pairs for a given
dataset id and rank. It is the caller’s responsibility to ensure the tag
name does not collide with another key in the filemap.

To assign a tag (string) and value (another string) to a dataset.

::

     scr_filemap_set_tag(map, dset, rank, tag, value);

To retrieve the value associated with a tag.

::

     char* value = scr_filemap_get_tag(map, dset, rank, tag);

To unset a tag value.

::

     scr_filemap_unset_tag(map, dset, rank, tag);

Accessing a filemap file
~~~~~~~~~~~~~~~~~~~~~~~~

A filemap can be serialized to a file. The following functions write a
filemap to a file and read a filemap from a file.

Write the specified filemap to a file.

::

     scr_filemap_write(filename, map);

Read contents from a filemap file and merge into specified filemap
object.

::

     scr_filemap_read(filename, map);
