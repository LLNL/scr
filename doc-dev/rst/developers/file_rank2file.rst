.. _rank2file_file:

Rank2file map
-------------

The rank2file map tracks which files were written by which ranks during
a particular dataset. This map contains information for every rank and
file. For large jobs, it may consist of more bytes than can be loaded
into any single MPI process. This information is scattered among
multiple files that are organized as a tree. These files are stored in
the dataset directory on the parallel file system. Internally, the data
of the rank2file map is organized as a hash.

There is always a root file named ``rank2file.scr``. Here are the
contents of an example root rank2file map.

::

     LEVEL
       1
     RANKS
       4
     RANK
       0
         OFFSET
           0
         FILE
           .scr/rank2file.0.0.scr

Note that there is no ``VERSION`` field. The version is implied from the
summary file for the dataset. The ``LEVEL`` field lists the level at
which the current rank2file map is located in the tree. The leaves of
the tree are at level 0. The ``RANKS`` field specifies the number of
ranks the current file (and its associated subtree) contains information
for.

For levels that are above level 0, the ``RANK`` hash contains
information about other rank2file map files to be read. Each entry in
this hash is identified by a rank id, and then for each rank, a ``FILE``
and ``OFFSET`` are given. The rank id specifies which rank is
responsible for reading content at the next level. The ``FILE`` field
specifies the file name that is to be read, and the ``OFFSET`` field
gives the starting byte offset within that file.

A process reading a file at the current level scatters the hash info to
the designated “reader” ranks, and those processes read data for the
next level. In this way, the task of reading the rank2file map is
distributed among multiple processes in the job. The SCR library ensures
that the maximum amount of data any process reads in any step is limited
(currently 1MB).

File names at levels lower than the root have names of the form
``rank2file.<level>.<rank>.scr``, where ``level`` is the level number
within the tree and ``rank`` is the rank of the process that wrote the
file.

Finally, level 0 contains the data that maps a rank to a list of files
names. Here are the contents of an example rank2file map file at level
0.

::

     RANK2FILE
       LEVEL
         0
       RANKS
         4
       RANK
         0
           FILE
             rank_0.ckpt
               SIZE
                 524294
               CRC
                 0x6697d4ef
         1
           FILE
             rank_1.ckpt
               SIZE
                 524295
               CRC
                 0x28eeb9e
         2
           FILE
             rank_2.ckpt
               SIZE
                 524296
               CRC
                 0xb6a62246
         3
           FILE
             rank_3.ckpt
               SIZE
                 524297
               CRC
                 0x213c897a

Again, the number of ranks that this file contains information for is
recorded under the ``RANKS`` field.

There are entries for specific ranks under the ``RANK`` hash, which is
indexed by rank id within ``scr_comm_world``. For a given rank, each
file that rank wrote as part of the dataset is indexed by file name
under the ``FILE`` hash. The file name specifies the relative path to
the file starting from the dataset directory. For each file, SCR records
the size of the file in bytes under ``SIZE``, and SCR may also record
the CRC32 checksum value over the contents of the file under the ``CRC``
field.

On restart, the reader rank that reads this hash scatters the
information to the owner rank, so that by the end of processing the
tree, all processes know which files to read.
