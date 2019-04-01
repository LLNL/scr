Launch a run
------------

``scripts/TLCC/scr_run.in``
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Prepares a resource allocation for SCR, launches a run, re-launches on
failures, and scavenges and rebuilds files for most recent checkpoint if
needed. Updates SCR index file in prefix directory to account for last
checkpoint.

#. Interprets ``$SCR_ENABLE``, calls ``srun`` and bails if set to 0.

#. Interprets ``$SCR_DEBUG``, enables verbosity if set :math:`>` 0.

#. Invokes ``scr_test_runtime`` to check that runtime dependencies are
   available.

#. Invokes “``scr_env –jobid``” to get jobid of current job.

#. Interprets ``$SCR_NODELIST`` to determine set of nodes job is using,
   sets and exports ``$SCR_NODELIST`` to value returned by
   “``scr_env –nodes``” if not set.

#. Invokes ``$scr_prefix`` to get prefix directory on parallel file
   system.

#. Interprets ``$SCR_WATCHDOG``.

#. Invokes ``scr_glob_hosts`` to check that this command is running on a
   node in the nodeset, bails with error if not.

#. Invokes ``scr_list_dir`` to get control directory.

#. Issues a NOP ``srun`` command on all nodes to force each node to run
   SLURM prologue to delete old files from cache.

#. Invokes ``scr_prerun`` to prepare nodes for SCR run.

#. If ``$SCR_FLUSH_ASYNC == 1``, invokes ``scr_glob_hosts`` to get count
   of number of nodes. and invokes ``srun`` to launch an
   ``scr_transfer`` process on each node.

ENTER LOOP

#. Invokes ``scr_list_down_nodes`` to determine list of bad nodes. If
   any node has been previously marked down, force it to continue to be
   marked down. We do this to avoid re-running on “bad” nodes, the logic
   being that if a node was identified as being bad in this resource
   allocation once already, there is a good chance that it is still bad
   (even if it currently seems to be healthy), so avoid it.

#. Invokes ``scr_list_down_nodes`` to print reason for down nodes, if
   any.

#. Count the number of nodes that the application needs. Invokes
   ``scr_glob_hosts`` to count number of nodes in ``$SCR_NODELIST``,
   which lists all nodes in allocation. Interprets ``$SCR_MIN_NODES`` to
   use that value of set, otherwise invokes ``scr_env –runnodes`` to get
   number of nodes used in last run.

#. Invokes ``scr_glob_host`` to count number of nodes left in the
   allocation.

#. If number of nodes left is smaller than number needed, break loop.

#. Invokes ``scr_glob_host`` to ensure node running ``scr_srun`` script
   is not listed as a down node, if it is, break loop.

#. Build list of nodes to be excluded from run.

#. Optionally log start of run.

#. Invokes ``srun`` including node where the ``scr_srun`` command is
   running and excluding down nodes.

#. If watchdog is enabled, record pid of srun, invokes ``sleep 10`` so
   job shows up in squeue, invokes ``scr_get_jobstep_id`` to get SLURM
   jobstep id from pid, invokes ``scr_watchdog`` and records pid of
   watchdog process.

#. Invokes ``scr_list_down_nodes`` to get list of down nodes.

#. Optionally log end of run (and down nodes and reason those nodes are
   down).

#. If number of attempted runs is :math:`>=` than number of allowed
   retries, break loop.

#. Invokes ``scr_retries_halt`` and breaks loop if halt condition is
   detected.

#. Invokes “``sleep 60``” to give nodes in allocation a chance to
   cleanup.

#. Invokes ``scr_retries_halt`` and breaks loop if halt condition is
   detected. We do this a second time in case a command to halt came in
   while we were sleeping.

#. Loop back.

EXIT LOOP

#. If ``$SCR_FLUSH_ASYNC == 1``, invokes “``scr_halt –immediate``” to
   kill ``scr_transfer`` processes on each node.

#. Invokes ``scr_postrun`` to scavenge most recent checkpoint.

#. Invokes ``kill`` to kill watchdog process if it is running.

``scripts/common/scr_test_runtime.in``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Checks that various runtime dependencies are available.

#. Checks for ``pdsh`` command,

#. Checks for ``dshbak`` command,

#. Checks for ``Date::Manip`` perl module.

``scripts/common/scr_prerun.in``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Executes commands to prepare an allocation for SCR.

#. Interprets ``$SCR_ENABLE``, calls ``srun`` and bails if set to 0.

#. Interprets ``$SCR_DEBUG``, enables verbosity if set :math:`>` 0.

#. Invokes ``scr_test_runtime`` to check for necessary run time
   dependencies.

#. Invokes ``mkdir`` to create ``.scr`` subdirectory in prefix
   directory.

#. Invokes ``rm -f`` to remove flush and nodes files from prefix
   directory.

#. Returns 0 if allocation is ready, 1 otherwise.

``src/scr_retries_halt.c``
~~~~~~~~~~~~~~~~~~~~~~~~~~

Reads halt file and returns exit code depending on whether the run
should be halted or not.
