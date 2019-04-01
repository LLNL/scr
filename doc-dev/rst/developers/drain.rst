.. _drain:

Scavenge
========

At the end of an allocation, certain SCR commands inspect the cache to
verify that the most recent checkpoint has been copied to the parallel
file system. If not, these commands execute other SCR commands to
scavenge this checkpoint before the allocation ends. In this section, we
detail key concepts referenced as part of the scavenge operations.
Detailed program flow for these operations is provided in
Section :ref:`Program Flow>Scavenge <flow_drain>`.

Rank filemap file
-----------------

The ``scr_copy`` command is a serial program (non-MPI) that executes on
a compute node and copies all files belonging to a specified dataset id
from the cache to a specified dataset directory on the parallel file
system. It is implemented in ``scr_copy.c`` whose program flow is
described in Section :ref:`<scr_copy>`. The
``scr_copy`` command copies all application files and SCR redundancy
data files. In addition, it writes a special filemap file for each rank
to the dataset directory. The name of this filemap file is of the
format: ``<rank>.scrfilemap``. An example hash for such a filemap file
is shown below:

::

     DSET
       6
         RANK
           2
     RANK
       2
         DSET
           6
             DSETDESC
               COMPLETE
                 1
               SIZE
                 2097182
               FILES
                 4
               ID
                 6
               NAME
                 scr.dataset.6
               CREATED
                 1312850690668536
               USER
                 user1
               JOBNAME
                 simulation123
               JOBID
                 112573
               CKPT
                 6
             FILES
               2
             FILE
               3_of_4_in_0.xor
                 META
                   RANKS
                     4
                   COMPLETE
                     1
                   SIZE
                     175693
                   TYPE
                     XOR
                   FILE
                     3_of_4_in_0.xor
                   CRC
                     0x2ef519a1
               rank_2.ckpt
                 META
                   COMPLETE
                     1
                   SIZE
                     524296
                   NAME
                     rank_2.ckpt
                   PATH
                     /p/lscratchb/user1/simulation123
                   ORIG
                     rank_2.ckpt
                   RANKS
                     4
                   TYPE
                     FULL
                   FILE
                     rank_2.ckpt
                   CRC
                     0x738bb68f

It lists the files owned by a rank for a particular dataset. In this
case, it shows that rank ``2`` wrote two files (``FILES=2``) as part of
dataset id ``6``. Those files are named ``rank_2.ckpt`` and
``3_of_4_in_0.xor``.

This format is similar to the filemap hash format described in
Section :ref:`Filemap <filemap>`. The main differences are
that files are listed using relative paths instead of absolute paths and
there are no redundancy descriptors. The paths are relative so that the
dataset directory on the parallel file system may be moved or renamed.
Redundancy descriptors are cache-specific, so these entries are
excluded.

Scanning files
--------------

After ``scr_copy`` copies files from the cache on each compute node to
the parallel file system, the ``scr_index`` command runs to check
whether all files were recovered, rebuild missing files if possible, and
add an entry for the dataset to the SCR index file
(Section :ref:`Index_file <index_file>`). When invoking the
``scr_index`` command, the full path to the prefix directory and the
name of the dataset directory are specified on the command line. The
``scr_index`` command is implemented in ``scr_index.c``, and its program
flow is described in Section  :ref:`<scr_index>`.

The ``scr_index`` command first acquires a listing of all items
contained in the dataset directory by calling ``scr_read_dir``, which is
implemented in ``scr_index.c``. This function uses POSIX calls to list
all files and subdirectories contained in the dataset directory. The
hash returned by this function distinguishes directories from files
using the following format.

::

     DIR
       <dir1>
       <dir2>
       ...
     FILE
       <file1>
       <file2>
       ...

The ``scr_index`` command then iterates over the list of file names and
reads each file that ends with the “``.scrfilemap``” extension. These
files are the filemap files written by ``scr_copy`` as described above.
The ``scr_index`` command records the number of expected files for each
rank into a single hash called the *scan hash*.

For each file listed in the rank filemap file, the ``scr_index`` command
verifies the meta data from the rank filemap map against the original
file (excluding CRC32 checks). If the file passes these checks, the
command adds a corresponding entry for the file to the scan hash. This
entry is formatted such that it can be used as an entry in the summary
file hash (Section :ref:`Summary file <summary_file>`). If the
file is an ``XOR`` file, it sets a ``NOFETCH`` flag under the ``FILE``
key, which instructs the SCR library to exclude this file during a fetch
operation.

Furthermore, for each ``XOR`` file, the ``scr_index`` command extracts
info about the ``XOR`` set from the file name and adds an entry under an
``XOR`` key in the scan hash. It records the ``XOR`` set id (under
``XOR``), the number of members in the set (under ``MEMBERS``), and the
group rank of the current file in this set (under ``MEMBER``), as well
as, the global rank id (under ``RANK``) and the name of the ``XOR`` file
(under ``FILE``). After this all of this, the scan hash might look like
the following example:

::

   DLIST
     <dataset_id>
       DSET
         COMPLETE
           1
         SIZE
           2097182
         FILES
           4
         ID
           6
         NAME
           scr.dataset.6
         CREATED
           1312850690668536
         USER
           user1
         JOBNAME
           simulation123
         JOBID
           112573
         CKPT
           6
       RANK2FILE
         RANKS
           <num_ranks>
         RANK
           <rank1>
             FILES
               <num_expected_files_for_rank1>
             FILE
                <filename>
                  SIZE
                    <filesize>
                  CRC
                    <crc>
                <xor_filename>
                  NOFETCH
                  SIZE
                    <filesize>
                  CRC
                    <crc>
                ...
           <rank2>
             FILES
               <num_expected_files_for_rank2>
             FILE
                <filename>
                  SIZE
                    <filesize>
                  CRC
                    <crc>
                <xor_filename>
                  NOFETCH
                  SIZE
                    <filesize>
                  CRC
                    <crc>
                ...
           ...
       XOR
         <set1>
           MEMBERS
             <num_members_in_set1>
           MEMBER
             <member1>
               FILE
                 <xor_filename_of_member1_in_set1>
               RANK
                 <rank_id_of_member1_in_set1>
             <member2>
               FILE
                 <xor_filename_of_member2_in_set1>
               RANK
                 <rank_id_of_member2_in_set1>
             ...
         <set2>
           MEMBERS
             <num_members_in_set2>
           MEMBER
             <member1>
               FILE
                 <xor_filename_of_member1_in_set2>
               RANK
                 <rank_id_of_member1_in_set2>
             <member2>
               FILE
                 <xor_filename_of_member2_in_set2>
               RANK
                 <rank_id_of_member2_in_set2>
             ...
         ...

Inspecting files
----------------

After merging data from all filemap files in the dataset directory, the
``scr_index`` command inspects the scan hash to identify any missing
files. For each dataset, it determines the number of ranks associated
with the dataset, and it checks that it has an entry in the scan hash
for each rank. It then checks whether each rank has as an entry for each
of its expected number of files. If any file is determined to be
missing, the command adds an ``INVALID`` flag to the scan hash, and it
lists all ranks that are missing files under the ``MISSING`` key. This
operation may thus add entries like the following to the scan hash.

::

   DLIST
     <dataset_id>
       INVALID
       MISSING
         <rank1>
         <rank2>
         ...

Rebuilding files
----------------

If any ranks are missing files, then the ``scr_index`` command attempts
to rebuild files. Currently, only the ``XOR`` redundancy scheme can be
used to rebuild files. The command iterates over each of the ``XOR``
sets listed in the scan hash, and it checks that each set has an entry
for each of its members. If it finds an ``XOR`` set that is missing a
member, or if it finds that a set contains a rank which is known to be
missing files, the command constructs a string that can be used to fork
and exec a process to rebuild the files for that process. It records
these strings under the ``BUILD`` key in the scan hash. If it finds that
one or more files cannot be recovered, it sets an ``UNRECOVERABLE`` flag
in the scan hash. If the ``scr_index`` command determines that it is
possible to rebuild all missing files, it forks and execs a process for
each string listed under the ``BUILD`` hash. Thus this operation may add
entries like the following to the scan hash.

::

   DLIST
     <dataset_id>
       UNRECOVERABLE
       BUILD
         <cmd_to_rebuild_files_for_set1>
         <cmd_to_rebuild_files_for_set2>
         ...

Scan hash
---------

After all of these steps, the scan hash is of the following form:

::

   DLIST
     <dataset_id>
       UNRECOVERABLE
       BUILD
         <cmd_to_rebuild_files_for_set1>
         <cmd_to_rebuild_files_for_set2>
         ...
       INVALID
       MISSING
         <rank1>
         <rank2>
         ...
       RANKS
         <num_ranks>
       RANK
         <rank>
           FILES
             <num_files_to_expect>
           FILE
             <file_name>
               SIZE
                 <size_in_bytes>
               CRC
                 <crc32_string_in_0x_form>
             <xor_file_name>
               NOFETCH
               SIZE
                 <size_in_bytes>
               CRC
                 <crc32_string_in_0x_form>
             ...
         ...
       XOR
         <xor_setid>
           MEMBERS
             <num_members_in_set>
           MEMBER
             <member_id>
               FILE
                 <xor_filename>
               RANK
                 <rank>
             ...
         ...

After the rebuild attempt, the ``scr_index`` command writes a summary
file in the dataset directory. To produce the hash for the summary file,
the command deletes extraneous entries from the scan hash
(``UNRECOVERABLE``, ``BUILD``, ``INVALID``, ``MISSING``, ``XOR``) and
adds the summary file format version number.
