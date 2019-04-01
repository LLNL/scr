.. _flow_drain:

Scavenge
--------

SCR commands should be executed after the final run of the application
in a resource allocation to check that the most recent checkpoint is
successfully copied to the parallel file system before exiting the
allocation. This logic is encapsulated in the ``scr_postrun`` command.

``scripts/common/scr_postrun.in``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Checks whether there is a dataset in cache that must be copied to the
parallel file system. If so, scavenge this dataset, rebuild any missing
files if possible, and finally update SCR index file in prefix
directory.

#. Interprets ``$SCR_ENABLE``, bails with success if set to 0.

#. Interprets ``$SCR_DEBUG``, enables verbosity if set :math:`>` 0.

#. Invokes ``scr_prefix`` to determine prefix directory on parallel file
   system (but this value is overridden via “-p” option when called from
   ``scr_srun``).

#. Interprets ``$SCR_NODELIST`` to determine set of nodes job is using,
   invokes ``scr_env –nodes`` if not set.

#. Invokes ``scr_list_down_nodes`` to determine which nodes are down.

#. Invokes ``scr_glob_hosts`` to subtract down nodes from node list to
   determine which nodes are still up, bails with error if there are no
   up nodes left.

#. Invokes ``scr_list_dir control`` to get the control directory.

#. Invokes “``scr_flush_file –dir $pardir –latest``” providing prefix
   directory to determine id of most recent dataset.

#. If this command fails, there is no dataset to scavenge, so bail out
   with error.

#. Invokes “``scr_inspect –up $UPNODES –from $cntldir``” to get list of
   datasets in cache.

#. Invokes “``scr_flush_file –dir $pardir –needflush $id``” providing
   prefix directory and dataset id to determine whether this dataset
   needs to be copied.

#. If this command fails, the dataset has already been flushed, so bail
   out with success.

#. Invokes “``scr_flush_file –dir $pardir –subdir $id``” to get name for
   dataset directory.

#. Creates dataset directory on parallel file system, and creates hidden
   ``.scr`` directory.

#. Invokes ``scr_scavenge`` providing control directory, dataset id to
   be copied, dataset directory, and set of known down nodes, which
   copies dataset files from cache to the PFS.

#. Invokes ``scr_index`` providing dataset directory, which checks
   whether all files are accounted for, attempts to rebuild missing
   files if it can, and records the new directory and status in the SCR
   index file.

#. If dataset was copied and indexed successfully, marks the dataset as
   current in the index file.

``scripts/TLCC/scr_scavenge.in``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Executes within job batch script to manage scavenge of files from cache
to parallel file system. Uses ``scr_hostlist.pm`` and ``scr_param.pm``.

#. Uses ``scr_param.pm`` to read ``SCR_FILE_BUF_SIZE`` (sets size of
   buffer when writing to file system).

#. Uses ``scr_param.pm`` to read ``SCR_CRC_ON_FLUSH`` (flag indicating
   whether to compute CRC32 on file during scavenge).

#. Uses ``scr_param.pm`` to read ``SCR_USE_CONTAINERS`` (flag indicating
   whether to combine application files into container files).

#. Invokes “``scr_env –jobid``” to get jobid.

#. Invokes “``scr_env –nodes``” to get the current nodeset, can override
   with “``–jobset``” on command line.

#. Logs start of scavenge operation, if logging is enabled.

START ROUND 1

#. Invokes ``pdsh`` of ``scr_copy`` providing control directory, dataset
   id, dataset directory, buffer size, CRC32 flag, partner flag,
   container flag, and list of known down nodes.

#. Directs ``stdout`` to one file, directs ``stderr`` to another.

#. Scan ``stdout`` file to build list of partner nodes and list of nodes
   where copy command failed.

#. Scan ``stderr`` file for a few well-known error strings indicating
   pdsh failed.

#. Build list of all failed nodes and list of nodes that were partners
   to those failed nodes, if any.

#. If there were any nodes that failed in ROUND 1, enter ROUND 2.

END ROUND 1, START ROUND 2

#. Build list of updated failed nodes, includes nodes known to be failed
   before ROUND 1, plus any nodes detected as failed in ROUND 1.

#. Invokes ``pdsh`` of ``scr_copy`` on partner nodes of failed nodes (if
   we found a partner for each failed node) or on all non-failed nodes
   otherwise, provided the updated list of failed nodes.

END ROUND 2

#. Logs end of scavenge, if logging is enabled.

.. _scr_copy:

``src/scr_copy.c``
~~~~~~~~~~~~~~~~~~

Serial process that runs on a compute node and copies files for
specified dataset to parallel file system.

#. Read control directory, dataset id, destination dataset directory,
   etc from command line.

#. Read master filemap and each filemap it lists.

#. If specified dataset id does not exist, we can’t copy it so bail out
   with error.

#. Loop over each rank we have for this dataset.

RANK LOOP

#. Get flush descriptor from filemap. Record partner if set and whether
   we should preserve user directories or use containers.

#. If partner flag is set, print node name of partner and loop.

#. Otherwise, check whether we have all files for this rank, and if not
   loop to next rank.

#. Otherwise, we’ll actually start to copy files.

#. Allocate a rank filemap object and set expected number of files.

#. Copy dataset hash into rank filemap.

#. Record whether we’re preserving user directories or using containers
   in the rank filemap.

#. Loop over each file for this rank in this dataset.

FILE LOOP

#. Get file name.

#. Check that we can read the file, if not record an error state.

#. Get file meta data from filemap.

#. Check whether file is application or SCR file.

#. If user file, set destination directory. If preserving directories,
   get user-specified directory from meta data and call mkdir.

#. Create destination file name.

#. Copy file from cache to destination, optionally compute CRC32 during
   copy.

#. Compute relative path to destination file from dataset directory.

#. Add relative path to file to rank filemap.

#. If CRC32 was enabled and also was set on original file, check its
   value, or if it was not already set, set it in file meta data.

#. Record file meta data in rank filemap.

#. Free temporaries.

END FILE LOOP

#. Write rank filemap to dataset directory.

#. Delete rank filemap object.

END RANK LOOP

#. Free path to dataset directory and hidden ``.scr`` directory.

#. Print and exit with code indicating success or error.

.. _scr_index:

``src/scr_index.c``
~~~~~~~~~~~~~~~~~~~

Given a dataset directory as command line argument, checks whether
dataset is indexed and adds to index if not. Attempts to rebuild missing
files if needed.

#. If “``–add``” option is specified, call ``index_add_dir``
   (Section :ref:`0.1.5 <flow_index_add_dir>`) to add directory to
   index file.

#. If “``–remove``” option is specified, call ``index_remove_dir`` to
   delete dataset directory from index file. Does not delete associated
   files, only the reference to the directory from the index file.

#. If “``–current``” option is specified, call ``index_current_dir`` to
   mark specified dataset directory as current. When a dataset is marked
   as current, SCR attempts to restart the job from that dataset and
   works backwards if it fails.

#. If “``–list``” option is specified, call ``index_list`` to list
   contents of index file.

.. _flow_index_add_dir:

``index_add_dir``
~~~~~~~~~~~~~~~~~

Adds specified dataset directory to index file, if it doesn’t already
exist. Rebuilds files if possible, and writes summary file if needed.

#. Read index file.

#. Lookup dataset directory in index file, if it’s already indexed, bail
   out with success.

#. Otherwise, concatenate dataset subdirectory name with prefix
   directory to get full path to the dataset directory.

#. Attempt to read summary file from dataset directory. Call
   ``scr_summary_build``
   (Section :ref:`0.1.6 <flow_index_summary_build>`) if it does not
   exist.

#. Read dataset id from summary file, if this fails exit with error.

#. Read completeness flag from summary file.

#. Write entry to index hash for this dataset, including directory name,
   dataset id, complete flag, and flush timestamp.

#. Write hash out as new index file.

.. _flow_index_summary_build:

``scr_summary_build``
~~~~~~~~~~~~~~~~~~~~~

Scans all files in dataset directory, attempts to rebuild files, and
writes summary file.

#. If we can read the summary file, bail out with success.

#. Call ``scr_scan_files``
   (Section :ref:`0.1.7 <flow_index_scan_files>`) to read meta data
   for all files in directory. This records all data in a scan hash.

#. Call ``scr_inspect_scan``
   (Section :ref:`0.1.9 <flow_index_inspect_scan>`) to examine whether
   all files in scan hash are complete, and record results in scan hash.

#. If files are missing, call ``scr_rebuild_scan``
   (Section :ref:`0.1.10 <flow_index_rebuild_scan>`) to attempt to
   rebuild files. After the rebuild, we delete the scan hash, rescan,
   and re-inspect to produce an updated scan hash.

#. Delete extraneous entries from scan hash to form our summary file
   hash (Section :ref:`Summary file <summary_file>`).

#. Write out summary file.

.. _flow_index_scan_files:

``scr_scan_files``
~~~~~~~~~~~~~~~~~~

Reads all filemap and meta data files in directory to build a hash
listing all files in dataset directory.

#. Build string to hidden ``.scr`` subdirectory in dataset directory.

#. Build regular expression to identify ``XOR`` files.

#. Open hidden directory.

BEGIN LOOP

#. Call ``readdir`` to get next directory item.

#. Get item name.

#. If item does not end with “``.scrfilemap``”, loop.

#. Otherwise, create full path to file name.

#. Call ``scr_scan_file`` to read file into scan hash.

#. Free full path and loop to next item.

END LOOP

.. _flow_index_scan_file:

``scr_scan_file``
~~~~~~~~~~~~~~~~~

#. Create new rank filemap object.

#. Read filemap.

#. For each dataset id in filemap...

#. Get dataset id.

#. Get scan hash for this dataset.

#. Lookup rank2file map in scan hash, or create one if it doesn’t exist.

#. For each rank in this dataset...

#. Get rank id.

#. Read dataset hash from filemap and record in scan hash.

#. Get rank hash from rank2file hash for the current rank.

#. Set number of expected files.

#. For each file for this rank and dataset...

#. Get file name.

#. Build full path to file.

#. Get meta data for file from rank filemap.

#. Read number of ranks, file name, file size, and complete flag for
   file.

#. Check that file exists.

#. Check that file size matches.

#. Check that number of ranks we expect matches number from meta data,
   use this value to set the expected number of ranks if it’s not
   already set.

#. If any check fails, skip to next file.

#. Otherwise, add entry for this file in scan hash.

#. If meta data is for an ``XOR`` file, add an ``XOR`` entry in scan
   hash.

.. _flow_index_inspect_scan:

``scr_inspect_scan``
~~~~~~~~~~~~~~~~~~~~

Checks that each rank has an entry in the scan hash, and checks that
each rank has an entry for each of its expected number of files.

#. For each dataset in scan hash, get dataset id and pointer to its hash
   entries.

#. Lookup rank2file hash under ``RANK2FILE`` key.

#. Lookup hash for ``RANKS`` key, and check that we have exactly one
   entry.

#. Read number of ranks for this dataset.

#. Sort entries for ranks in scan hash by rank id.

#. Set expected rank to 0, and iterate over each rank in loop.

BEGIN LOOP

#. Get rank id and hash entries for current rank.

#. If rank id is invalid or out of order compared to expected rank,
   throw an error and mark dataset as invalid.

#. While current rank id is higher than expected rank id, mark expected
   rank id as missing and increment expected rank id.

#. Get ``FILES`` hash for this rank, and check that we have exactly one
   entry.

#. Read number of expected files for this rank.

#. Get hash of file names for this rank recorded in ``scr_scan_files``.

#. For each file, if it is marked as incomplete, mark rank as missing.

#. If number of file entries for this rank is less than expected number
   of files, mark rank as missing.

#. If number of file entries for this rank is more than expected number
   of files, mark dataset as invalid.

#. Increment expected rank id.

END LOOP

#. While expected rank id is less than the number of ranks for this
   dataset, mark expected rank id as missing and increment expected rank
   id.

#. If expected rank id is more than the number of ranks for this
   dataset, mark dataset as invalid.

#. Return ``SCR_SUCCESS`` if and only if we have all files for each
   dataset.

.. _flow_index_rebuild_scan:

``scr_rebuild_scan``
~~~~~~~~~~~~~~~~~~~~

Identifies whether any files are missing and forks and execs processes
to rebuild missing files if possible.

#. Iterate over each dataset id recorded in scan hash.

#. Get dataset id and its hash entries.

#. Look for flag indicating that dataset is invalid. We assume the
   dataset is bad beyond repair if we find such a flag.

#. Check whether there are any ranks listed as missing files for this
   dataset, if not, go to next dataset.

#. Otherwise, iterate over entries for each ``XOR`` set.

BEGIN LOOP

#. Get ``XOR`` set id and number of members for this set.

#. Iterate over entries for each member in the set. If we are missing an
   entry for the member, or if we have its entry but its associated rank
   is listed as one of the missing ranks, mark this member as missing.

#. If we are missing files for more than one member of the set, mark the
   dataset as being unrecoverable. In this case, we won’t attempt to
   rebuild any files.

#. Otherwise, if we are missing any files for the set, build the string
   that we’ll use later to fork and exec a process to rebuild the
   missing files.

END LOOP

#. If dataset is recoverable, call ``scr_fork_rebuilds`` to fork and
   exec processes to rebuild missing files. This forks a process for
   each missing file where each invokes ``scr_rebuild_xor`` utility,
   implemented in ``scr_rebuild_xor.c``. If any of these rebuild
   processes fail, then consider the rebuild as failed.

#. Return ``SCR_SUCCESS`` if and only if, for each dataset id in the
   scan hash, the dataset is not explicitly marked as bad, and all files
   already existed or we were able to rebuild all missing files.
