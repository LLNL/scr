SCR_Init
--------

During ``SCR_Init()``, the library allocates and initializes data
structures. It inspects the cache, and it distributes and rebuilds files
for cached datasets. Otherwise, it attempts to fetch the most recent
checkpoint from the parallel file system. This function is implemented
in ``scr.c``.

#. Interprets ``$SCR_ENABLE``, if not enabled, bail out with error.

#. Calls ``DTCMP_Init`` if datatype compare library is available.

#. Create ``scr_comm_world`` by duplicating ``MPI_COMM_WORLD`` and set
   ``scr_my_rank_world`` and ``scr_ranks_world``.

#. Call ``scr_env_nodename`` to get hostname, store in
   ``scr_my_hostname``.

#. Call ``getpagesize`` to get memory page size, store in
   ``scr_page_size``.

#. Initialize parameters – rank 0 reads any config files and broadcasts
   info to other ranks.

#. Check whether we are still enabled (a config file may disable us),
   and bail out with error if not.

#. Check that ``scr_username`` and ``scr_jobid`` are defined, which are
   used for logging purposes.

#. Call ``scr_groupdescs_create`` to create group descriptors
   (Section :ref:`0.1.1 <groupdesc_create>`).

#. Call ``scr_storedescs_create`` to create store descriptors
   (Section :ref:`0.1.2 <storedesc_create>`).

#. Call ``scr_reddescs_create`` to create redundancy descriptors
   (Section :ref:`0.1.3 <reddesc_create>`).

#. Check that we have a valid redundancy descriptor that we can use for
   each checkpoint.

#. Create ``scr_comm_node`` and ``scr_comm_node_across``.

#. Log the start of this run, if logging is enabled.

#. Verify that ``scr_prefix`` is defined.

#. Define ``scr_prefix_scr`` to be ``.scr`` directory within prefix
   directory.

#. Create ``.scr`` directory in prefix directory.

#. Define control directory and save in ``scr_cntl_prefix``.

#. Create the control directory.

#. Create each of the cache directories.

BARRIER

#. Define file names for halt, flush, nodes, transfer, and filemap
   files.

#. Delete any existing transfer file.

#. Create nodes file, write total number of nodes in the job (max of
   size of ``scr_comm_level``).

#. Allocate a hash to hold halt status, and initialize halt seconds if
   needed.

BARRIER

#. Stop any ongoing asynchronous flush.

#. Check whether we need to halt and exit this run.

#. Enable ``scr_flush_on_restart`` and disable ``scr_fetch`` if
   ``scr_global_restart`` is set.

#. Create empty hash for filemap.

#. Master process for each store reads and distributes info from
   filemaps (Section :ref:`0.1.4 <distribute_filemap>`).

#. Call ``scr_cache_rebuild`` to rebuild datasets in cache
   (Section :ref:`0.1.5 <rebuild_loop>`).

#. If rebuild was successful, call ``scr_flush_file_rebuild`` to update
   flush file.

#. If rebuild successful, check whether dataset should be flushed.

#. If we don’t have a checkpoint (rebuild failed), call
   ``scr_cache_purge`` to clear cache (delete all files).

#. If fetch is enabled, call ``scr_fetch_sync`` to read checkpoint from
   parallel file system (Section :ref:`0.1.12 <fetch_loop>`).

#. If the fetch failed, clear the cache again.

BARRIER

#. Log end of initialization.

#. Start timer to record length of compute phase and log start of
   compute phase.

.. _groupdesc_create:

scr_groupdescs_create
~~~~~~~~~~~~~~~~~~~~~

The group descriptors are kept in the ``scr_groupdescs`` array
(Section :ref:`Group descriptors] <:group_descriptors>`). This
function is implemented in ``scr_groupdesc.c``.

#. Read ``GROUPS`` key from ``scr_groupdesc_hash`` which is set while
   processing parameters in ``SCR_Init``.

#. Count number of groups and add two.

#. Allocate space for ``scr_groupdesc`` array.

#. Initialize each group descriptor.

#. Create descriptor for all tasks on the same node called ``NODE``.

#. Create descriptor for all tasks in job called ``WORLD``.

#. Create each group descriptor specified in ``scr_groupdesc_hash`` by
   calling ``scr_groupdesc_create_by_str``. Rank 0 broadcasts the group
   name to determine the order.

The ``scr_groupdesc_create_by_str`` function creates groups of processes
by splitting ``scr_comm_world`` into subcommunicators containing all
procs that specify the same string. The real work is delegated to
``scr_rank_str``, which is implemented in ``scr_split.c``. It executes a
bitonic sort on string names, and it returns the number of distinct
groups across all procs and the group id to which the calling process
belongs. This id is then used in a call to ``MPI_Comm_split``.

.. _storedesc_create:

scr_storedescs_create
~~~~~~~~~~~~~~~~~~~~~

The store descriptors are kept in the ``scr_storedescs`` array
(Section :ref:`Store descriptors <store_descriptors>`). This
function is implemented in ``scr_storedesc.c``.

#. Read ``STORE`` key from ``scr_storedesc_hash`` which is set while
   processing parameters in ``SCR_Init``.

#. Count number of store descriptors.

#. Allocate space for ``scr_storedescs`` array.

#. Sort store descriptors to ensure they are in the same order on all
   procs.

#. Create each store descriptor specified in ``scr_storedesc_hash`` by
   calling ``scr_storedesc_create_from_hash``.

#. Create store descriptor for control directory and save it in
   ``scr_storedesc_cntl``.

The ``scr_storedesc_create`` function sets all fields in the descriptor
using default values or values defined in the hash. A key field is a
communicator consisting of the group of processes that share the
associated storage device. This communicator is used to coordinate
processes when accessing the device. It is created by duplicating a
communicator from a group descriptor.

.. _reddesc_create:

scr_reddescs_create
~~~~~~~~~~~~~~~~~~~

The redundancy descriptors are kept in the ``scr_reddescs`` array
(Section :ref:`Redundancy descriptors <redundancy_descriptors>`).
This function is implemented in ``scr_reddesc.c``.

#. Read ``CKPT`` key from ``scr_reddesc_hash`` which is set while
   processing parameters in ``SCR_Init``.

#. Count number of redundancy descriptors.

#. Allocate space for ``scr_reddescs`` array.

#. Sort redundancy descriptors to ensure they are in the same order on
   all procs.

#. Create each redundancy descriptor specified in ``scr_reddesc_hash``
   by calling ``scr_reddesc_create_from_hash``.

The ``scr_reddesc_create_from_hash`` function sets all fields in the
descriptor from default values or values defined in the hash. Two key
fields consist of an index to the store descriptor providing details on
the class of storage to use and a communicator on which to compute
redundancy data. To build the communicator, a new communicator is
created by splitting ``scr_comm_world`` into subcommunicators consisting
of processes from different failure groups.

.. _distribute_filemap:

scr_scatter_filemaps
~~~~~~~~~~~~~~~~~~~~

During a restart, the master process for each control directory reads in
all filemap data and distributes this data to the other processes
sharing the control directory, if any. After this distribution phase, a
process is responsible for each file it has filemap data for, and each
file in cache is the responsibility of some process. We use this
approach to handle cases where the number of tasks accessing the control
directory in the current run is different from the number of tasks in
the prior run. This function is implemented in ``scr_cache_rebuild.c``.

#. Master reads master filemap file.

#. Master creates empty filemap and reads each filemap file listed in
   the master filemap. Deletes each filemap file as it’s read.

#. Gather list of global rank ids sharing the store to master process.

#. If the filemap has data for a rank, master prepares hash to send
   corresponding data to that rank.

#. Master evenly distributes the remainder of the filemap data to all
   processes.

#. Distribute filemap data via ``scr_hash_exchange()``.

#. Master writes new master filemap file.

#. Each process writes new filemap file.

.. _rebuild_loop:

scr_cache_rebuild
~~~~~~~~~~~~~~~~~

This section describes the logic to distribute and rebuild files in
cache. SCR attempts to rebuild all cached datasets. This functionality
is implemented in ``scr_cache_rebuild.c``.

#. Start timer.

#. Delete any files from cache known to be incomplete.

#. Get list of dataset ids currently in cache.

LOOP

#. Identify dataset with lowest id across all procs yet to be rebuilt.

#. If there is no dataset id specified on any process, break loop.

#. Otherwise, log which dataset we are attempting to rebuild.

#. Distribute hash for this dataset and store in map object
   (Section :ref:`0.1.6 <distribute_dset_hash>`).

#. If we fail to distribute the hash to all processes, delete this
   dataset from cache and loop.

#. Distribute redundancy descriptors for this dataset and store in
   temporary redundancy descriptor object
   (Section :ref:`0.1.7 <distribute_reddesc>`). This informs each
   process about the cache device and the redundancy scheme to use for
   this dataset.

#. If we fail to distribute the redundancy descriptors to all processes,
   delete this dataset from cache and loop.

#. Create dataset directory in cache according to redundancy descriptor.

#. Distribute files to the ranks that wrote them
   (Section :ref:`0.1.8 <distribute_files>`). The owner ranks may now
   be on different nodes.

#. Rebuild any missing files for this dataset using redundancy scheme
   specified in redundancy descriptor
   (Section :ref:`0.1.9 <rebuild_files>`).

#. If the rebuild fails, delete this dataset from cache and loop.

#. Otherwise, the rebuild succeeded. Update ``scr_dataset_id`` and
   ``scr_checkpoint_id`` if the id for the current dataset is higher, so
   that we continue counting up from this number when assigning ids to
   later datasets.

#. Unset FLUSHING flag in flush file.

#. Free the temporary redundancy descriptor.

EXIT LOOP

#. Stop timer and log whether we were able to rebuild any dataset from
   cache.

.. _distribute_dset_hash:

scr_distribute_datasets
~~~~~~~~~~~~~~~~~~~~~~~

Given a filemap and dataset id, distribute dataset hash and store in
filemap.

#. Create empty send hash for transferring dataset hashes.

#. Get list of ranks that we have files for as part of the specified
   dataset.

#. For each rank, lookup dataset hash from filemap and add to send hash.

#. Delete list of ranks.

#. Check that no rank identified an invalid rank. If the restarted run
   uses a smaller number of processes than the previous run, we may (but
   are not guaranteed to) discover this condition here.

#. Identify smallest rank that has a copy of the dataset hash.

#. Return with failure if no such rank exists.

#. Otherwise, broadcast hash from this rank.

#. Store dataset hash in filemap and write filemap to disk.

#. Delete send hash.

.. _distribute_reddesc:

scr_distribute_reddescs
~~~~~~~~~~~~~~~~~~~~~~~

Given a filemap and dataset id, distribute redundancy descriptor that
was applied to the dataset and store in filemap. This creates the same
group and redundancy scheme that was applied to the dataset, even if the
user may have configured new schemes for the current run.

#. Create empty send hash for transferring redundancy descriptor hashes.

#. Get list of ranks that we have files for as part of the specified
   dataset.

#. For each rank, lookup redundancy descriptor hash from filemap and add
   to send hash.

#. Delete list of ranks.

#. Check that no rank identified an invalid rank. If the restarted run
   uses a smaller number of processes than the previous run, we may (but
   are not guaranteed to) discover this condition here.

#. Execute sparse data exchange with ``scr_hash_exchange``.

#. Check that each rank received its descriptor, return with failure if
   not.

#. Store redundancy descriptor hash in filemap and write filemap to
   disk.

#. Create redundancy descriptor by calling
   ``scr_reddesc_create_from_filemap``.

#. Delete send and receive hashes from exchange.

.. _distribute_files:

scr_distribute_files
~~~~~~~~~~~~~~~~~~~~

This section describes the algorithm used to distribute files for a
specified dataset. SCR transfers files from their current location to
the storage device accessible from the node where the owner rank is now
running. The algorithm operates over a number of rounds. In each round,
a process may send files to at most one other process. A process may
only send files if it has all of the files written by the owner process.
The caller specifies a filemap, a redundancy descriptor, and a dataset
id as input. This implementation is in ``scr_cache_rebuild.c``.

#. Delete all bad (incomplete or inaccessible) files from the filemap.

#. Get list of ranks that we have files for as part of the specified
   dataset.

#. From this list, set a start index to the position corresponding to
   the first rank that is equal to or greater than our own rank (looping
   back to rank 0 if we pass the last rank). We stagger the start index
   across processes in this way to help distribute load later.

#. Check that no rank identified an invalid rank while scanning for its
   start index. If the restarted run uses a smaller number of processes
   than the previous run, we may (but are not guaranteed to) discover
   this condition here.

#. Allocate arrays to record which rank we can send files to in each
   round.

#. Check that we have all files for each rank, and record the round in
   which we can send them. The round we pick here is affected by the
   start index computed earlier.

#. Issue sparse global exchange via ``scr_hash_exchange`` to inform each
   process in which round we can send it its files, and receive similar
   messages from other processes.

#. Search for minimum round in which we can retrieve our own files, and
   remember corresponding round and source rank. If we can fetch files
   from our self, we’ll always select this option as it will be the
   minimum round.

#. Free the list of ranks we have files for.

#. Determine whether all processes can obtain their files, and bail with
   error if not.

#. Determine the maximum round any process needs to get its files.

#. Identify which rank we’ll get our files from and issue sparse global
   exchange to distribute this info.

#. Determine which ranks want to receive files from us, if any, and
   record the round they want to receive their files in.

#. Get the directory name for this dataset.

#. Loop through the maximum number of rounds and exchange files.

LOOP ROUNDS

#. Check whether we can send files to a rank in this round, and if so,
   record destination and number of files.

#. Check whether we need to receive our files in this round, and if so,
   record source rank.

#. If we need to send files to our self, just move (rename) each file,
   update the filemap, and loop to the next round.

#. Otherwise, if we have files for this round but the the owner rank
   does not need them, delete them.

#. If we do not need to send or receive any files this round, loop to
   next round.

#. Otherwise, exchange number of files we’ll be sending and/or
   receiving, and record expected number that we’ll receive in our
   filemap.

#. If we’re sending files, get a list of files for the destination.

#. Enter exchange loop.

LOOP EXCHANGE

#. Get next file name from our list of files to send, if any remaining.

#. Swap file names with partners.

#. If we’ll receive a file in this iteration, add the file name to the
   filemap and write out our filemap.

#. Transfer file via ``scr_swap_files()``. This call overwrites the
   outgoing file (if any) with the incoming file (if any), so there’s no
   need to delete the outgoing file. If there is no incoming file, it
   deletes the outgoing file (if any). We use this approach to conserve
   storage space, since we assume the cache is small. We also transfer
   file metadata with this function.

#. If we sent a file, remove that file from our filemap and write out
   the filemap.

#. Decrement the number of files we have to send / receive by one. When
   both counts hit zero, break exchange loop.

#. Write updated filemap to disk.

EXIT LOOP EXCHANGE

#. Free list of files that we sent in this round.

EXIT LOOP ROUNDS

#. If we have more ranks than there were rounds, delete files for all
   remaining ranks.

#. Write out filemap file.

#. Delete bad files (incomplete or inaccessible) from the filemap.

.. _rebuild_files:

scr_reddesc_recover
~~~~~~~~~~~~~~~~~~~

This function attempts to rebuild any missing files for a dataset. It
returns ``SCR_SUCCESS`` on all processes if successful; it returns
``!SCR_SUCCESS`` on all processes otherwise. The caller specifies a
filemap, a redundancy descriptor, and a dataset id as input. This
function is implemented in in ``scr_reddesc_recover.c``.

#. Attempt to rebuild files according to the redundancy scheme specified
   in the redundancy descriptor. Currently, only ``XOR`` can actually
   rebuild files (Section :ref:`0.1.10 <attempt_rebuild_files_xor>`).

#. If the rebuild failed, return with an error.

#. Otherwise, check that all processes have all of their files for the
   dataset.

#. If not, return with an error.

#. If so, reapply the redundancy scheme, if needed. No need to do this
   with ``XOR``, since it does this step as part of the rebuild.

.. _attempt_rebuild_files_xor:

scr_reddesc_recover_xor_attempt
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Before we attempt to rebuild files using the ``XOR`` redundancy scheme,
we first check whether it is possible. If we detect that two or more
processes from the same ``XOR`` set are missing files, we cannot recover
all files and there is no point to rebuild any of them. We execute this
check in ``scr_reddesc_recover.c``. The caller specifies a filemap, a
redundancy descriptor, and a dataset id as input.

#. Check whether we have our dataset files, and check whether we have
   our ``XOR`` file. If we’re missing any of these files, assume that
   we’re missing them all.

#. Count the number of processes in our ``XOR`` set that need their
   files. We can recover all files from a set so long as no more than a
   single member needs its files.

#. Check whether we can recover files for all sets, if not bail with an
   error.

#. If the current process is in a set which needs to be rebuilt,
   identify which rank needs its files and call
   ``scr_reddesc_recover_xor()`` to rebuild files
   (Section :ref:`0.1.11 <rebuild_files_xor>`).

#. Check that the rebuild succeeded on all tasks, return error if not,
   otherwise return success.

.. _rebuild_files_xor:

scr_reddesc_recover_xor
~~~~~~~~~~~~~~~~~~~~~~~

We invoke this routine within each ``XOR`` set that is missing files.
The caller specifies a filemap, a redundancy descriptor, and a dataset
id as input, as well as, the rank of the process in the ``XOR`` set that
is missing its files. We refer to the process that needs to rebuild its
files as the *root*. This function is implemented in
``scr_reddesc_recover.c``

ALL

#. Get pointer to ``XOR`` state structure from ``copy_state`` field of
   redundancy descriptor.

#. Allocate empty hash to hold the header of our ``XOR`` file.

NON-ROOT

#. Get name of our ``XOR`` file.

#. Open ``XOR`` file for reading.

#. Read header from file.

#. From header, get hash of files we wrote.

#. From this file hash, get the number of files we wrote.

#. Allocate arrays to hold file descriptor, file name, and file size for
   each of our files.

#. Get path of dataset directory from ``XOR`` file name.

#. Open each of our files for reading and store file descriptor, file
   name, and file size of each file in our arrays.

#. If the failed rank is to our left, send it our header. Our header
   stores a copy of the file hash for the rank to our left under the
   ``PARTNER`` key.

#. If the failed rank is to our right, send it our file hash. When the
   failed rank rebuilds its ``XOR`` file, it needs to record our file
   hash in its header under the ``PARTNER`` key.

ROOT

#. Receive ``XOR`` header from rank to our right.

#. Rename ``PARTNER`` key in this header to ``CURRENT``. The rank to our
   right stored a copy of our file hash under ``PARTNER``.

#. Receive file hash from rank to our left, and store it under
   ``PARTNER`` in our header.

#. Get our file hash from ``CURRENT`` key in the header.

#. From our file hash, get the number of files we wrote during the
   dataset.

#. Allocate arrays to hold file descriptor, file name, and file size for
   each of our files.

#. Build the file name for our ``XOR`` file, and add ``XOR`` file to the
   filemap.

#. For each of our files, get meta data from file hash, then get file
   name and file size from meta data. Add file name to filemap, and
   record file name and file size in arrays.

#. Record the number of files we expect to have in the filemap,
   including the ``XOR`` file.

#. Write out filemap.

#. Open ``XOR`` file for writing.

#. Open each of our dataset files for writing, and record file
   descriptors in our file descriptor array.

#. Write out ``XOR`` header to ``XOR`` file.

ALL

#. Read ``XOR`` chunk size from header.

#. Allocate buffers to send and receive data during reduction.

#. Execute pipelined ``XOR`` reduction to root to reconstruct missing
   data as illustrated in Figure :ref:`xor_reduce`. For a full description of
   the redundancy scheme, see Section :ref:`XOR algorithm <raid>`.

#. Close our ``XOR`` file.

#. Close each of our dataset files.

ROOT

#. For each of our dataset files and our ``XOR`` file, update filemap.

#. Write filemap to disk.

#. Also compute and record CRC32 checksum for each file if
   ``SCR_CRC_ON_COPY`` is set.

ALL

#. Free data buffers.

#. Free arrays for file descriptors, file names, and file sizes.

#. Free ``XOR`` header hash.

.. _fetch_loop:

scr_fetch_sync
~~~~~~~~~~~~~~

This section describes the loop used to fetch a checkpoint from the
parallel file system. SCR starts with the most recent checkpoint on the
parallel file system as specified in the index file. If SCR fails to
fetch this checkpoint, it then works backwards and attempts to fetch the
next most recent checkpoint until it either succeeds or runs out of
checkpoints. It acquires the list of available checkpoints from the
index file. This functionality is implemented within ``scr_fetch.c``.

#. Start timer.

#. Rank 0 reads index file from prefix directory, bail if failed to read
   file.

LOOP

#. Rank 0 selects a target directory name. Start with directory marked
   as current if set, and otherwise use most recent checkpoint specified
   in index file. For successive iterations, attempt the checkpoint that
   is the next most recent.

#. Rank 0 records fetch attempt in index file.

#. Rank 0 builds full path to dataset.

#. Broadcast dataset path from rank 0.

#. Attempt to fetch checkpoint from selected directory.

#. If fetch fails, rank 0 deletes “current” designation from dataset and
   marks dataset as “failed” in index file.

#. If fetch succeeds, rank 0 updates “current” designation to point to
   this dataset in index file, break loop.

EXIT LOOP

#. Delete index hash.

#. Stop timer and print statistics.

SCR_Need_checkpoint
-------------------

Determines whether a checkpoint should be taken. This function is
implemented in ``scr.c``.

#. If not enabled, bail out with error.

#. If not initialized, bail out with error.

#. Increment the ``scr_need_checkpoint_id`` counter. We use this counter
   so the user can specify that the application should checkpoint after
   every so many calls to ``SCR_Need_checkpoint``.

#. Check whether we need to halt. If so, then set need checkpoint flag
   to true.

#. Rank 0 checks various properties to make a decision: user has called
   ``SCR_Need_checkpoint`` an appropriate number of times, or the max
   time between consecutive checkpoints has expired, or the ratio of the
   total checkpoint time to the total compute time is below a threshold.

#. Rank 0 broadcasts the decision to all other tasks.

SCR_Start_checkpoint
--------------------

Prepares the cache for a new checkpoint. This function is implemented in
``scr.c``.

#. If not enabled, bail out with error.

#. If not initialized, bail out with error.

#. If this is being called from within a Start/Complete pair, bail out
   with error.

#. Issue a barrier here so that processes don’t delete checkpoint files
   from the cache before we’re sure that all processes will actually
   make it this far.

BARRIER

#. Stop timer of compute phase, and log this compute section.

#. Increment ``scr_dataset_id`` and ``scr_checkpoint_id``.

#. Get redundancy descriptor for this checkpoint id.

#. Start timer for checkpoint phase, and log start of checkpoint.

#. Get a list of all datasets in cache.

#. Get store descriptor associated with redundancy descriptor.

#. Determine how many checkpoints are currently in the cache directory
   specified by the store descriptor.

#. Delete oldest datasets from this directory until we have sufficient
   room for this new checkpoint. When selecting checkpoints to delete,
   skip checkpoints that are being flushed. If the only option is a
   checkpoint that is being flushed, wait for it to complete then delete
   it.

#. Free the list of checkpoints.

#. Rank 0 fills in the dataset descriptor hash and broadcasts it.

#. Store dataset hash in filemap.

#. Add flush descriptor entries to filemap for this dataset.

#. Store redundancy descriptor in filemap.

#. Write filemap to disk.

#. Create dataset directory in cache.

SCR_Route_file
--------------

Given a name of a file, return the string the caller should use to
access this file. This function is implemented in ``scr.c``.

#. If not enabled, bail out with error.

#. If not initialized, bail out with error.

#. Lookup redundancy descriptor for current checkpoint id.

#. Direct path to dataset directory in cache according to redundancy
   descriptor.

#. If called from within a Start/Complete pair, add file name to
   filemap. Record original file name as specified by caller, the
   absolute path to the file and the number of ranks in the job in the
   filemap. Update filemap on disk.

#. Otherwise, we assume we are in a restart, so check whether we can
   read the file, and return error if not. The goal in this case is to
   provide a mechanism for a process to determine whether it can read
   its checkpoint file from cache during a restart.

#. Return success.

SCR_Complete_checkpoint
-----------------------

Applies redundancy scheme to checkpoint files, may flush checkpoint to
parallel file system, and may exit run if the run should be halted. This
function is implemented in ``scr.c``.

#. If not enabled, bail out with error.

#. If not initialized, bail out with error.

#. If not called from within Start/Complete pair, bail out with error.

#. Record file size and valid flag for each file written during
   checkpoint.

#. Write out meta data for each file registered in filemap for this
   dataset id.

#. Compute total data size across all procs and determine whether all
   procs specified a valid write.

#. Update filemap and write to disk.

#. Verify that flush is valid by checking that all files belong to same
   subdirectory and compute container offsets if used.

#. Apply redundancy scheme specified in redundancy descriptor
   (Section :ref:`0.5.1 <copy_partner>` or
   Section :ref:`0.5.2 <copy_xor>`).

#. Stop timer measuring length of checkpoint, and log cost of
   checkpoint.

#. If checkpoint was successful, update our flush file, check whether we
   need to halt, and check whether we need to flush.

#. If checkpoint was not successful, delete it from cache.

#. Check whether any ongoing asynchronous flush has completed.

BARRIER

#. Start timer for start of compute phase, and log start of compute
   phase.

.. _copy_partner:

scr_reddesc_apply_partner
~~~~~~~~~~~~~~~~~~~~~~~~~

Algorithm to compute ``PARTNER`` redundancy scheme. Caller provides a
filemap, a redundancy descriptor, and a dataset id. This function is
implemented in ``scr_reddesc_apply.c``.

#. Get pointer to partner state structure from ``copy_state`` field in
   redundancy descriptor.

#. Read list of files for this rank for the specified checkpoint.

#. Inform our right-hand partner how many files we’ll send.

#. Record number of files we expect to receive from our left-hand
   partner in our filemap.

#. Remember the node name where our left-hand partner is running (used
   during scavenge).

#. Record the redundancy descriptor hash for our left-hand partner. Each
   process needs to be able to recover its own redundancy descriptor
   hash after a failure, so we make a copy in our partner’s filemap.

#. Write filemap to disk.

#. Get checkpoint directory we’ll copy partner’s files to.

#. While we have a file to send or receive, loop.

LOOP

#. If we have a file to send, get the file name.

#. Exchange file names with left-hand and right-hand partners.

#. If our left-hand partner will be sending us a file, add the file name
   to our filemap, and write out our filemap.

#. Exchange files by calling ``scr_swap_files()``, and update filemap
   with meta data for file.

EXIT LOOP

#. Write filemap to disk.

#. Free the list of file names.

.. _copy_xor:

scr_reddesc_apply_xor
~~~~~~~~~~~~~~~~~~~~~

Algorithm to compute ``XOR`` redundancy scheme. Caller provides a
filemap, a redundancy descriptor, and a dataset id. The ``XOR`` set is
the group of processes defined by the communicator specified in the
redundancy descriptor. This function is implemented in
``scr_reddesc_apply.c``.

#. Get pointer to ``XOR`` state structure from ``copy_state`` field in
   redundancy descriptor.

#. Allocate a buffers to send and receive data.

#. Count the number of files this process wrote during the specified
   dataset id. Allocate space to record a file descriptor, the file
   name, and the size of each file.

#. Record the redundancy descriptor hash for our left-hand partner in
   our filemap. Each process needs to be able to recover its own
   redundancy descriptor hash after a failure, so each process sends a
   copy to his right-hand partner.

#. Allocate a hash to hold the header of our ``XOR`` redundancy file.

#. Record the global ranks of the MPI tasks in our ``XOR`` set.

#. Record the dataset id in our header.

#. Open each of our files, get the size of each file, and read the meta
   data for each file.

#. Create a hash and record our rank, the number of files we have, and
   the meta data for each file.

#. Send this hash to our right-hand partner, and receive equivalent hash
   from left-hand partner.

#. Record our hash along with the hash from our left-hand partner in our
   ``XOR`` header hash. This way, the meta data for each file is
   recorded in the headers of two different ``XOR`` files.

#. Determine chunk size for RAID algorithm
   (Section :ref:`XOR algorithm <raid>`) and record this size in the
   ``XOR`` header.

#. Determine full path name for ``XOR`` file.

#. Record ``XOR`` file name in our filemap and update the filemap on
   disk.

#. Open the ``XOR`` file for writing.

#. Write header to file and delete header hash.

#. Execute RAID algorithm and write data to ``XOR`` file
   (Section :ref:`XOR algorithm <raid>`).

#. Close and fsync our ``XOR`` file and close each of our dataset files.

#. Free off scratch space memory and MPI buffers.

#. Write out meta data file for ``XOR`` file.

#. If ``SCR_CRC_ON_COPY`` is specified, compute CRC32 checksum of
   ``XOR`` file.

SCR_Finalize
------------

Shuts down the SCR library, flushes most recent checkpoint to the
parallel file system, and frees data structures. This function is
implemented in ``scr.c``.

#. If not enabled, bail out with error.

#. If not initialized, bail out with error.

#. Stop timer measuring length of compute phase.

#. Add reason for exiting to halt file. We assume the user really wants
   to stop once the application calls ``SCR_Finalize``. We add a reason
   to the halt file so we know not to start another run after we exit
   from this one.

#. Complete or stop any ongoing asynchronous flush.

#. Flush most recent checkpoint if we still need to.

#. Disconnect logging functions.

#. Free internal data structures.

#. Call ``DTCMP_Finalize`` if used.
