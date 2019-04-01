.. _flow_flush:

Flush
-----

This section describes the process of a synchronous flush.

scr_flush_sync
~~~~~~~~~~~~~~

This is implemented in ``scr_flush_sync.c``.

#. Return with failure if flush is disabled.

#. Return with success if specified dataset id has already been flushed.

#. Barrier to ensure all procs are ready to start.

#. Start timer to record flush duration.

#. If async flush is in progress, wait for it to stop. Then check that
   our dataset still needs to be flushed.

#. Log start of flush.

#. Add FLUSHING marker for dataset in flush file to denote flush
   started.

#. Get list of files to flush, identify containers, create directories,
   and create containers (Section :ref:`0.1.2 <flow_flush_prepare>`).
   Store list in new hash.

#. Flush data to files or containers
   (Section :ref:`0.1.7 <flow_flush_data>`).

#. Write summary file (Section :ref:`0.1.9 <flow_flush_complete>`).

#. Get total bytes from dataset hash in filemap.

#. Delete hashes of data and list of files.

#. Removing FLUSHING marker from flush file.

#. Stop timer, compute bandwidth, log end.

.. _flow_flush_prepare:

scr_flush_prepare
~~~~~~~~~~~~~~~~~

Given a filemap and dataset id, prepare and return a list of files to be
flushed, also create corresponding directories and container files. This
is implemented in ``scr_flush.c``.

#. Build hash of files, directories, and containers for flush
   (Section :ref:`0.1.3 <flow_flush_identify>`).

#. Create directory tree for dataset
   (Section :ref:`0.1.6 <flow_flush_create_dirs>`).

#. Create container files in ``scr_flush_create_containers``.

   #. Loop over each file in file list hash. If the process writes to
      offset 0, have it open, create, truncate, and close the container
      file.

.. _flow_flush_identify:

scr_flush_identify
~~~~~~~~~~~~~~~~~~

Creates a hash of files to flush. This is implemented in
``scr_flush.c``.

#. Check that all procs have all of their files for this dataset.

#. Add files to file list hash, including meta data in
   ``scr_flush_identify_files``.

   #. Read dataset hash from filemap, add to file list hash.

   #. Loop over each file for dataset, if file is not ``XOR`` add it and
      its meta data to file list hash.

#. Add directories to file list hash
   (Section :ref:`0.1.4 <flow_flush_identify_dirs>`).

#. Add containers to file list hash in
   (Section :ref:`0.1.5 <flow_flush_identify_containers>`).

.. _flow_flush_identify_dirs:

scr_flush_identify_dirs
~~~~~~~~~~~~~~~~~~~~~~~

Specifies directories which must be created as part of flush, and
identifies processes responsible for creating them. This is implemented
in ``scr_flush.c``.

#. Extract dataset hash from file list hash.

#. If we’re preserving user directories:

   #. Allocate arrays to call DTCMP to rank directory names of all
      files.

   #. For each file, check that its user-specified path is under the
      prefix directory, insert dataset directory name in subdir array,
      and insert full parent directory in dir array.

   #. Check that all files from all procs are within a directory under
      the prefix directory.

   #. Call DTCMP with subdir array to rank user directory names for all
      files, and check that one common dataset directory contains all
      files.

   #. Broadcast dataset directory name to all procs.

   #. Record dataset directory in file list hash.

   #. Call DTCMP with the dir array to rank all directories across all
      procs.

   #. For each unique directory, we pick one process to later create
      that directory. This process records the directory name in the
      file list hash.

   #. Free arrays.

#. Otherwise (if we’re not preserving user-defined directories):

   #. Get name of dataset from dataset hash.

   #. Append dataset name to prefix directory to define dataset
      directory.

   #. Record dataset directory in file list hash.

   #. Record dataset directory as destination path for each file in file
      list hash.

.. _flow_flush_identify_containers:

scr_flush_identify_containers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For each file to be flushed in file list hash, identify segments,
containers, offsets, and length.s This is implemented in
``scr_flush.c``.

#. Get our rank within the ``scr_comm_node`` communicator.

#. Get the container size.

#. Extract dataset hash from file list hash.

#. Define path within dataset directory to all container files.

#. Loop over each file in file list hash and compute total byte count.

#. Compute total bytes across all processes in run with allreduce on
   ``scr_comm_world``.

#. Compute total bytes per node by reducing to node leader in
   ``scr_comm_node``.

#. Compute offset for each node with a scan across node leaders in
   ``scr_comm_node_across``.

#. Compute offset of processes within each node with scan within
   ``scr_comm_node``.

#. Loop over each file and compute offset of each file.

#. Given the container size, the offset and length of each file, compute
   container file name, length, and offset for each segment and store
   details within file list hash.

#. Check that all procs identified their containers.

.. _flow_flush_create_dirs:

scr_flush_create_dirs
~~~~~~~~~~~~~~~~~~~~~

Given a file list hash, create dataset directory and any subdirectories
to hold dataset. This is implemented in ``scr_flush.c``.

#. Get file mode for creating directories.

#. Rank 0 creates the dataset directory:

   #. Read path from file list hash.

   #. Get subdirectory name of dataset within prefix directory.

   #. Extract dataset hash from file list hash, and get dataset id.

   #. Add dataset directory and id to index file, write index file to
      disk.

   #. Create dataset directory and its hidden ``.scr`` subdirectory.

#. Barrier across all procs.

#. Have each leader create its directory as designated in
   Section :ref:`0.1.4 <flow_flush_identify_dirs>`.

#. Ensure that all directories were created.

.. _flow_flush_data:

scr_flush_data
~~~~~~~~~~~~~~

This is implemented in ``scr_flush_sync.c``. To flow control the number
of processes writing, rank 0 writes its data first and then serves as a
gate keeper. All processes wait until they receive a go ahead message
from rank 0 before starting, and each sends a message back to rank 0
when finished. Rank 0 maintains a sliding window of active writers. Each
process includes a flag indicating whether it failed or succeeded to
copy its files. If rank 0 detects that a process fails, the go ahead
message it sends to other writers indicates this failure, in which that
writer immediate sends back a message without copying its files. This
way time is not wasted by later writers if an earlier writer has already
failed.

RANK 0

#. Flush files in list, writing data to containers if used
   (Section :ref:`0.1.8 <flow_flush_list>`).

#. Allocate arrays to manage a window of active writers.

#. Send “go ahead” message to first W writers.

#. Waitany for any writer to send completion notification, record flag
   indicating whether that writer was successful, and send “go ahead”
   message to next writer.

#. Loop until all writers have completed.

#. Execute allreduce to inform all procs whether flush was successful.

NON-RANK 0

#. Wait for go ahead message.

#. Flush files in list, writing data to containers if used
   (Section :ref:`0.1.8 <flow_flush_list>`).

#. Send completion message to rank 0 indicating whether copy succeeded.

#. Execute allreduce to inform all procs whether flush was successful.

.. _flow_flush_list:

scr_flush_files_list
~~~~~~~~~~~~~~~~~~~~

Given a list of files, this function copies data file-by-file, and then
it updates the hash that forms the rank2file map. It is implemented in
``scr_flush_sync.c``.

#. Get path to summary file from file list hash.

#. Loop over each file in file list.

LOOP

#. Get file name.

#. Get basename of file (throw away directory portion).

#. Get hash for this file.

#. Get file meta data from hash.

#. Check for container segments (TODO: what if a process has no files?).

CONTAINERS

#. Add basename to rank2file map.

#. Flush file to its containers.

#. If successful, record file size, CRC32 if computed, and segment info
   in rank2file map.

#. Otherwise, record 0 for COMPLETE flag in rank2file map.

#. Delete file name and loop.

NON-CONTAINERS

#. Get directory to write file from PATH key in file hash.

#. Append basename to directory to get full path.

#. Compute relative path to file starting from dataset directory.

#. Add relative path to rank2file map.

#. Copy file data to destination.

#. If successful, copy file size and CRC32 if computed in rank2file map.

#. Otherwise, record 0 for COMPLETE flag in rank2file map.

#. Delete relative and full path names and loop.

END LOOP

.. _flow_flush_complete:

scr_flush_complete
~~~~~~~~~~~~~~~~~~

Writes out summary and rank2file map files. This is implemented in
``scr_flush.c``.

#. Extract dataset hash from file list hash.

#. Get dataset directory path.

#. Write summary file (Section :ref:`0.1.10 <flow_flush_summary>`).

#. Update index file to mark dataset as “current”.

#. Broadcast signal from rank 0 to indicate whether flush succeeded.

#. Update flush file that dataset is now on parallel file system.

.. _flow_flush_summary:

scr_flush_summary
~~~~~~~~~~~~~~~~~

Produces summary and rank2file map files in dataset directory on
parallel file system. Data for the rank2file maps are gathered and
written via a data-dependent tree, such that no process has to write
more than 1MB to each file. This is implemented in ``scr_flush.c``.

#. Get path to dataset directory and hidden ``.scr`` directory.

#. Given data to write to rank2file map file, pick a writer process so
   that each writer gets at most 1MB.

#. Call ``scr_hash_exchange_direction`` to fold data up tree.

#. Rank 0 creates summary file and writes dataset hash.

#. Define name of rank2file map files.

#. Funnel rank2file data up tree in recursive manner
   (Section :ref:`0.1.11 <flow_flush_summary_map>`).

#. If process is a writer, write rank2file map data to file.

#. Free temporaries.

#. Check that all procs wrote all files successfully.

.. _flow_flush_summary_map:

scr_flush_summary_map
~~~~~~~~~~~~~~~~~~~~~

Produces summary and rank2file map files in dataset directory on
parallel file system. This is implemented in ``scr_flush.c``.

#. Get path to dataset directory and hidden ``.scr`` directory.

#. If we received rank2file map in the previous step, create hash to
   specify its file name to include at next level in tree.

#. Given this hash, pick a writer process so that each writer gets at
   most 1MB.

#. Call ``scr_hash_exchange_direction`` to fold data up tree.

#. Define name of rank2file map files.

#. Funnel rank2file data up tree by calling ``scr_flush_summary_map``
   recursively..

#. If process is a writer, write rank2file map data to file.

#. Free temporaries.
