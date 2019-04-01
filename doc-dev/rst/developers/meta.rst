.. _meta:

Meta data
=========

Overview
--------

The ``scr_meta`` data structure associates various properties with files
written by the application and with redundancy data files written by
SCR. It tracks information such as the type of file (application vs. SCR
redundancy data), whether the application marked the file as valid or
invalid, the expected file size, its CRC32 checksum value if computed,
and the original string the application used to register the file.
Because the meta data is stored within a filemap
(Section :ref:`Filemap <filemap>`), there is no need to store
the dataset id or rank id which owns the file.

The ``scr_meta`` data structure makes heavy use of the ``scr_hash`` data
structure (Section :ref:`Hash <hash>`). The ``scr_hash`` is
utilized in the ``scr_meta`` API and its implementation. Essentially,
``scr_meta`` objects are specialized ``scr_hash`` objects, which have
certain well-defined keys (*fields*) and associated functions to access
those fields.

Example meta data hash
----------------------

Internally, meta data objects are implemented as ``scr_hash`` objects.
Here is an example hash for a meta data object containing information
for a file named “rank_0.ckpt”.

::

     FILE
       rank_0.ckpt
     TYPE
       FULL
     COMPLETE
       1
     SIZE
       524294
     CRC
       0x1b39e4e4
     CKPT
       6
     RANKS
       4
     ORIG
       ckpt.6/rank_0.ckpt
     ORIGPATH
       /p/lscratchb/user3/simulation123/ckpt.6
     ORIGNAME
       rank_0.ckpt

The ``FILE`` field records the file name this meta data associates with.
In this example, the file name is recorded using a relative path. The
``TYPE`` field indicates whether the file is written by the application
(``FULL``), whether it’s a ``PARTNER`` copy of a file (``PARTNER``), or
whether it’s a redundancy file for an ``XOR`` set (``XOR``). The
``COMPLETE`` field records whether the file is valid. It is set to 1 if
the file is thought to be valid, and 0 otherwise. The ``SIZE`` field
records the size of the file in bytes. The ``CRC`` field records the
CRC32 checksum value over the contents of the file. The ``CKPT`` field
records the checkpoint id in which the file was written. The ``RANKS``
field record the number of ranks active in the run when the file was
created. The ``ORIG`` field records the original string specified by the
caller when the file was registered in the call to ``SCR_Route_File()``.
The ``ORIGPATH`` field records the absolute path to the original file at
the time the file was registered, and the ``ORIGNAME`` field records
just the name of the file when registered.

In this case, “``rank_0.ckpt``” was created during checkpoint id 6, and
it was written in a run with 4 MPI tasks. It was written by the
application, and it is marked as being complete. It consists of 524,294
bytes and its CRC32 value is ``0x1b39e4e4``. The caller referred to this
file as “``ckpt.6/rank_0.ckpt``” when registering this file in
``SCR_Route_file()``. Based on the current working directory at the time
when ``SCR_Route_file`` was called, the absolute path to the file would
have been “``/p/lscratchb/user3/simulation123/ckpt.6``” and its name
would have been “``rank_0.ckpt``”.

These are the most common fields used in meta data objects. Not all
fields are required, and additional fields may be used that are not
shown here.

Common functions
----------------

This section describes some of the most common meta data functions. For
a detailed list of all functions, see ``scr_meta.h``. The implementation
can be found in ``scr_meta.c``.

Allocating, freeing, and copying meta data objects
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a new meta data object.

::

     scr_meta* meta = scr_meta_new()

Free a meta data object.

::

     scr_meta_delete(&meta)

Make an exact copy of ``meta_2`` in ``meta_1``.

::

     scr_meta_copy(meta_1, meta_2)

Setting, getting, and checking field values
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There are functions to set each field individually.

::

     scr_meta_set_complete(meta, complete)
     scr_meta_set_ranks(meta, ranks)
     scr_meta_set_checkpoint(meta, ckpt)
     scr_meta_set_filesize(meta, filesize)
     scr_meta_set_filetype(meta, filetype)
     scr_meta_set_filename(meta, filename)
     scr_meta_set_crc32(meta, crc32)
     scr_meta_set_orig(meta, string)
     scr_meta_set_origpath(meta, path)
     scr_meta_set_origname(meta, name)

If a field was already set to a value before making this call, the new
value overwrites any existing value.

And of course there are corresponding functions to get values.

::

     scr_meta_get_complete(meta, complete)
     scr_meta_get_ranks(meta, ranks)
     scr_meta_get_checkpoint(meta, ckpt)
     scr_meta_get_filesize(meta, filesize)
     scr_meta_get_filetype(meta, filetype)
     scr_meta_get_filename(meta, filename)
     scr_meta_get_crc32(meta, crc32)
     scr_meta_get_orig(meta, string)
     scr_meta_get_origpath(meta, path)
     scr_meta_get_origname(meta, name)

If the corresponding field is set, the get functions copy the value into
the output parameter and return ``SCR_SUCCESS``. If ``SCR_SUCCESS`` is
not returned, the output parameter is not changed.

Many times one simply wants to verify that a field is set to a
particular value. The following functions return ``SCR_SUCCESS`` if a
field is set and if that field matches the specified value.

::

     scr_meta_check_ranks(meta, ranks)
     scr_meta_check_checkpoint(meta, ckpt)
     scr_meta_check_filesize(meta, filesize)
     scr_meta_check_filetype(meta, filetype)
     scr_meta_check_filename(meta, filename)

Similar to the above functions, the following function returns
``SCR_SUCCESS`` if the complete field is set and if its value is set to
1.

::

     scr_meta_check_complete(meta)
