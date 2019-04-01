.. _flush_file:

Flush file
----------

The flush file tracks where cached datasets are located. It is stored in
the prefix directory. Internally, the data of the flush file is
organized as a hash. Here are the contents of an example flush file.

::

     DSET
       18
         LOCATION
           PFS
           CACHE
         DIR
           scr.dataset.18
       17
         LOCATION
           CACHE
         DIR
           scr.dataset.17

Each dataset is indexed by dataset id under the ``DSET`` hash. Then,
under the ``LOCATION`` hash, different flags are set to indicate where
that dataset is stored. The ``PFS`` flag indicates that a copy of this
dataset is stored on the parallel file system, while the ``CACHE`` flag
indicates that the dataset is stored in cache. The same dataset may be
stored in multiple locations at the same time. The ``DIR`` field
specifies the dataset directory name that SCR should use when copying
the dataset to the prefix directory on the parallel file system. At the
end of a run, the flush and scavenge logic in SCR uses information in
this file to determine whether or not the most recent checkpoint has
been copied to the parallel file system.
