.. _sec-config:

Configure a job
===============

The default SCR configuration suffices for many Linux clusters.
However, significant performance improvement or additional functionality
may be gained via custom configuration.

Setting parameters
------------------

SCR searches the following locations in the following order for a parameter value,
taking the first value it finds.

* Environment variables,
* User configuration file,
* Values set with :code:`SCR_Config`,
* System configuration file,
* Compile-time constants.

A convenient method to set an SCR parameter is through an environment variable, e.g.,::

  export SCR_CACHE_SIZE=2

In cases where SCR parameters need to be set based
on the run time configuration of the application,
the application can call :code:`SCR_Config`, e.g.,::

  SCR_Config("SCR_CACHE_SIZE=2");

Section :ref:`sec-integration-config` lists common use cases for :code:`SCR_Config`.

SCR also offers two configuration files:
a user configuration file and a system configuration file.
The user configuration file is useful for parameters that may need to vary by job,
while the system configuration file is useful for parameters that apply to all jobs.

To find a user configuration file,
SCR looks for a file named :code:`.scrconf` in the prefix directory.
Alternatively, one may specify the name and location of the user configuration file
by setting the :code:`SCR_CONF_FILE` environment variable at run time, e.g.,::

  export SCR_CONF_FILE=~/myscr.conf

The location of the system configuration file is hard-coded into SCR at build time.
This defaults to :code:`<install>/etc/scr.conf`.
One may choose a different path using the :code:`SCR_CONFIG_FILE` CMake option, e.g.,::

  cmake -DSCR_CONFIG_FILE=/path/to/scr.conf ...

To set an SCR parameter in a configuration file,
list the parameter name followed by its value separated by an '=' sign.
Blank lines are ignored, and any characters following the '#' comment character are ignored.
For example, a configuration file may contain something like the following::

  >>: cat ~/myscr.conf
  # set the halt seconds to one hour
  SCR_HALT_SECONDS=3600
  
  # set SCR to flush every 20 checkpoints
  SCR_FLUSH=20

One can include environment variable expressions in the value of SCR configuration parameters.
SCR interpolates the value of the environment variable at run time before setting the parameter.
This is useful for some parameters like storage paths,
which may only be defined within the allocation environment, e.g.,::

  # SLURM system that creates a /dev/shm directory for each job
  SCR_CNTL_BASE=/dev/shm/$SLURM_JOBID
  SCR_CACHE_BASE=/dev/shm/$SLURM_JOBID

.. _sec-config-common:

Common configurations
---------------------

This section describes some common configuration values.
These parameters can be set using any of the methods described above.

Enable debug messages
^^^^^^^^^^^^^^^^^^^^^

SCR can print informational messages about its operations, timing, and bandwidth::

  SCR_DEBUG=1

This setting is recommended during development and debugging.

Specify the job output directory
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

By default, SCR uses the current working directory as its prefix directory.
If one needs to specify a different path, set :code:`SCR_PREFIX`::

  SCR_PREFIX=/job/output/dir

It is common to set :code:`SCR_PREFIX` to be the top-level output directory
of the application.

Specify which checkpoint to load
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

By default, SCR attempts to load the most recent checkpoint.
If one wants to specify a particular checkpoint,
one can name which checkpoint to load by setting :code:`SCR_CURRENT`::

  SCR_CURRENT=ckptname

The value for the name must match the string that was given as the dataset name
during the call to :code:`SCR_Start_output` in which the checkpoint was created.

File-per-process vs shared access
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Applications achieve the highest performance when only
a single process accesses each file within a dataset.
This mode is termed *file-per-process*.
In that situation, SCR can keep files in cache locations
that might include node-local storage.

SCR also supports applications that require shared access to files,
where more than one process writes to or reads from a given file.
This mode is termed *shared access*.
To support shared access to a file,
SCR locates files in global storage like the parallel file system.
 
Regardless of the type of file access,
one can only use cache when there is sufficient capacity
to store the application files and associated SCR redundancy data.

There are several common SCR configurations depending on the needs of the application.

Write file-per-process, read file-per-process
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In this mode, an application uses file-per-process mode
both while writing its dataset during checkpoint/output
and while reading its dataset during restart.
So long as there is sufficient cache capacity,
SCR can use cache including node-local storage for both operations.
To configure SCR for this mode::

  SCR_CACHE_BYPASS=0

One must set :code:`SCR_CACHE_BYPASS=0` to instruct SCR to use cache.

Write file-per-process, read with shared access
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It is somewhat common for an application to write datasets using file-per-process
mode but then require shared access mode to read its checkpoint files during restart.
For example, there might be a top-level file that all processes read.
In this case, SCR can be configured to use cache like node-local storage while writing,
but it must be configured to move files to the prefix directory for restarts::

  SCR_CACHE_BYPASS=0
  SCR_GLOBAL_RESTART=1

Setting :code:`SCR_GLOBAL_RESTART=1` instructs SCR to rebuild any cached datasets
during :code:`SCR_Init` and then flush them to the prefix directory to read during
the restart phase.

Write with shared access
^^^^^^^^^^^^^^^^^^^^^^^^

If an application requires shared access mode while writing its dataset,
SCR must be configured to locate files on a global file system.
In this case, it is best to use the global file system both
for writing datasets during checkpoint/output and for reading files during restart::

  SCR_CACHE_BYPASS=1

Setting :code:`SCR_CACHE_BYPASS=1` instructs SCR to locate files
within the prefix directory for both checkpoint/output and restart phases.

Cache bypass mode must also be used when the cache capacity
is insufficient to store the application files and SCR redundancy data.

Because cache bypass mode is the most portable across different systems and applications,
it is enabled by default.

Change checkpoint flush frequency
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

By default, SCR flushes any dataset marked as :code:`SCR_FLAG_OUTPUT`,
and it flushes every 10th checkpoint.
To flush non-output checkpoint datasets at a different rate,
one can set :code:`SCR_FLUSH`.
For example, to flush every checkpoint::

  SCR_FLUSH=1

Change cache location
^^^^^^^^^^^^^^^^^^^^^

By default, SCR uses :code:`/dev/shm` as its cache base.
One can use a different cache location
by setting :code:`SCR_CACHE_BASE`.
For example, one might target a path
that points to a node-local SSD::

  SCR_CACHE_BASE=/ssd

This parameter is useful in runs that use a single cache location.
When using multiple cache directories within a single run,
one can define store and checkpoint descriptors as described later.

Change control and cache location
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

At times, one may need to set both the control and cache directories.
For example, some sites configure SLURM
to create a path to temporary storage for each allocation::

  SCR_CNTL_BASE=/tmp/$SLURM_JOBID
  SCR_CACHE_BASE=/tmp/$SLURM_JOBID

Another use case is when one needs to run multiple, independent
SCR jobs within a single allocation.
This is somewhat common in automated testing frameworks
that run many different test cases in parallel within
a single resource allocation.
To support this, one can configure each run
to use its own control and cache directories::

  # for test case 1
  SCR_CNTL_BASE=/dev/shm/test1
  SCR_CACHE_BASE=/dev/shm/test1

  # for test case 2
  SCR_CNTL_BASE=/dev/shm/test2
  SCR_CACHE_BASE=/dev/shm/test2

Increase cache size
^^^^^^^^^^^^^^^^^^^

When using cache, SCR stores at most one dataset by default.
One can increase this limit with :code:`SCR_CACHE_SIZE`,
e.g., to cache up to two datasets::

  SCR_CACHE_SIZE=2

It is recommended to use a cache size of at least 2 when possible.

Change redundancy schemes
^^^^^^^^^^^^^^^^^^^^^^^^^

By default, SCR uses the :code:`XOR` redundancy scheme
to withstand node failures.
One can change the scheme using the :code:`SCR_COPY_TYPE` parameter.
For example, to use Reed-Solomon to withstand up to two failures per set::

  SCR_COPY_TYPE=RS

In particular, on stable systems where one is using SCR primarily for
its asynchronous flush capability rather than for its fault tolerance,
it may be best to use :code:`SINGLE`::

  SCR_COPY_TYPE=SINGLE

It is possible to use multiple redundancy schemes in a single job.
For this, one must specify checkpoint descriptors as described in :ref:`sec-descriptors`.

Enable asynchronous flush
^^^^^^^^^^^^^^^^^^^^^^^^^

By default, SCR flushes datasets synchronously.
In this mode, the SCR API call that initiates the flush
does not return until the flush completes.
One can configure SCR to use asynchronous flushes instead,
in which case the flush is started during one SCR API call,
and it may be finalized in a later SCR API call.
To enable asynchronous flushes,
one should both set :code:`SCR_FLUSH_ASYNC=1`
and specify a flush type like :code:`PTHREAD`::

  SCR_FLUSH_ASYNC=1
  SCR_FLUSH_TYPE=PTHREAD

Restart with a different number of processes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To restart an application with a different number of processes than used to save the checkpoint,
one must follow the steps listed in :ref:`sec-integration-restart-without`.
Additionally, one should set the following::

  SCR_FLUSH_ON_RESTART=1
  SCR_FETCH=0

.. _sec-descriptors:

Group, store, and checkpoint descriptors
----------------------------------------

SCR must have information about process groups,
storage devices, and redundancy schemes.
SCR defines defaults that are sufficient in most cases.

By default, SCR creates a group of all processes in the job called :code:`WORLD`
and another group of all processes on the same compute node called :code:`NODE`.

For storage,
SCR requires that all processes be able to access the prefix directory,
and it assumes that :code:`/dev/shm` is storage local to each compute node.

SCR defines a default checkpoint descriptor that
caches datasets in :code:`/dev/shm` and protects against
compute node failure using the :code:`XOR` redundancy scheme.

The above defaults provide reasonable settings for Linux clusters.
If necessary, one can define custom settings via group, store,
and checkpoint descriptors in configuration files.

If more groups are needed, they can be defined in configuration files
with entries like the following::

  GROUPS=host1  POWER=psu1  SWITCH=0
  GROUPS=host2  POWER=psu1  SWITCH=1
  GROUPS=host3  POWER=psu2  SWITCH=0
  GROUPS=host4  POWER=psu2  SWITCH=1

Group descriptor entries are identified by a leading :code:`GROUPS` key.
Each line corresponds to a single compute node,
where the hostname of the compute node is the value of the :code:`GROUPS` key.
There must be one line for every compute node in the allocation.
It is recommended to specify groups in the system configuration file,
since these group definitions often apply to all jobs on the system.

The remaining values on the line specify a set of group name / value pairs.
The group name is the string to be referenced by store and checkpoint descriptors.
The value can be an arbitrary character string.
All nodes that specify the same value are placed in the same group.
Each unique value defines a distinct group.

In the above example, there are four compute nodes:
:code:`host1`, :code:`host2`, :code:`host3`, and :code:`host4`.
There are two groups defined: :code:`POWER` and :code:`SWITCH`.
Nodes :code:`host1` and :code:`host2` belong to one :code:`POWER` group (:code:`psu1`),
and nodes :code:`host3` and :code:`host4` belong to another (:code:`psu2`).
For the :code:`SWITCH` group,
nodes :code:`host1` and :code:`host3` belong to one group (:code:`0`),
and nodes :code:`host2` and :code:`host4` belong to another (:code:`1`).

Additional storage can be described in configuration files
with entries like the following::

  STORE=/dev/shm      GROUP=NODE   COUNT=1
  STORE=/ssd          GROUP=NODE   COUNT=3  FLUSH=PTHREAD
  STORE=/dev/persist  GROUP=NODE   COUNT=1  ENABLED=1  MKDIR=0
  STORE=/p/lscratcha  GROUP=WORLD

Store descriptor entries are identified by a leading :code:`STORE` key.
Each line corresponds to a class of storage devices.
The value associated with the :code:`STORE` key is the
directory prefix of the storage device.
This directory prefix also serves as the name of the store descriptor.
All compute nodes must be able to access their respective storage
device via the specified directory prefix.

The remaining values on the line specify properties of the storage class.
The :code:`GROUP` key specifies the group of processes that share a device.
Its value must specify a group name.
The :code:`GROUP` key is optional, and it defaults to :code:`NODE` if not specified.
The :code:`COUNT` key specifies the maximum number of datasets
that can be kept in the associated storage.
The user should be careful to set this appropriately
depending on the storage capacity and the application dataset size.
The :code:`COUNT` key is optional, and it defaults to the value
of the :code:`SCR_CACHE_SIZE` parameter if not specified.
The :code:`ENABLED` key enables (1) or disables (0) the store descriptor.
This key is optional, and it defaults to 1 if not specified.
The :code:`MKDIR` key specifies whether the device supports the
creation of directories (1) or not (0).
This key is optional, and it defaults to 1 if not specified.
The :code:`FLUSH` key specifies the transfer type to use when
flushing datasets from that storage location.
This key is optional, and it defaults to the value of the :code:`SCR_FLUSH_TYPE` if not specified.

In the above example, there are four storage devices specified:
:code:`/dev/shm`, :code:`/ssd`, :code:`/dev/persist`, and :code:`/p/lscratcha`.
The storage at :code:`/dev/shm`, :code:`/ssd`, and :code:`/dev/persist`
specify the :code:`NODE` group, which means that they are node-local storage.
Processes on the same compute node access the same device.
The storage at :code:`/p/lscratcha` specifies the :code:`WORLD` group,
which means that all processes in the job can access the device.
In other words, it is a globally accessible file system.

One can define checkpoint descriptors in a configuration file.
This is especially useful when more than one checkpoint descriptor
is needed in a single job.
Example checkpoint descriptor entries look like the following::

  # instruct SCR to use the CKPT descriptors from the config file
  SCR_COPY_TYPE=FILE
  
  # enable datasets to be stored in cache
  SCR_CACHE_BYPASS=0

  # the following instructs SCR to run with three checkpoint configurations:
  # - save every 8th checkpoint to /ssd using the PARTNER scheme
  # - save every 4th checkpoint (not divisible by 8) and any output dataset
  #   to /ssd using RS a set size of 8
  # - save all other checkpoints (not divisible by 4 or 8) to /dev/shm using XOR with
  #   a set size of 16
  CKPT=0 INTERVAL=1 GROUP=NODE   STORE=/dev/shm TYPE=XOR     SET_SIZE=16
  CKPT=1 INTERVAL=4 GROUP=NODE   STORE=/ssd     TYPE=RS      SET_SIZE=8  SET_FAILURES=3 OUTPUT=1
  CKPT=2 INTERVAL=8 GROUP=SWITCH STORE=/ssd     TYPE=PARTNER BYPASS=1

First, one must set the :code:`SCR_COPY_TYPE` parameter to :code:`FILE`.
Otherwise, SCR uses an implied checkpoint descriptor that is defined
using various SCR parameters including :code:`SCR_GROUP`, :code:`SCR_CACHE_BASE`,
:code:`SCR_COPY_TYPE`, and :code:`SCR_SET_SIZE`.

To store datasets in cache,
one must set :code:`SCR_CACHE_BYPASS=0` to disable bypass mode.
When bypass is enabled, all datasets are written directly to the parallel file system.

Checkpoint descriptor entries are identified by a leading :code:`CKPT` key.
The values of the :code:`CKPT` keys must be numbered sequentially starting from 0.
The :code:`INTERVAL` key specifies how often a descriptor is to be applied.
For each checkpoint,
SCR selects the descriptor having the largest interval value that evenly
divides the internal SCR checkpoint iteration number.
It is necessary that one descriptor has an interval of 1.
This key is optional, and it defaults to 1 if not specified.
The :code:`GROUP` key lists the failure group,
i.e., the name of the group of processes that are likely to fail at the same time.
This key is optional, and it defaults to the value of the
:code:`SCR_GROUP` parameter if not specified.
The :code:`STORE` key specifies the directory in which to cache the checkpoint.
This key is optional, and it defaults to the value of the
:code:`SCR_CACHE_BASE` parameter if not specified.
The :code:`TYPE` key identifies the redundancy scheme to be applied.
This key is optional, and it defaults to the value of the
:code:`SCR_COPY_TYPE` parameter if not specified.
The :code:`BYPASS` key indicates whether to bypass cache
and access data files directly on the parallel file system (1)
or whether to store them in cache (0).  In either case,
redundancy is applied to internal SCR metadata using the specified
descriptor settings.
This key is optional, and it defaults to the value of the
:code:`SCR_CACHE_BYPASS` parameter if not specified.

Other keys may exist depending on the selected redundancy scheme.
For :code:`XOR` and :code:`RS` schemes, the :code:`SET_SIZE` key specifies
the minimum number of processes to include in each redundancy set.
This defaults to the value of :code:`SCR_SET_SIZE` if not specified.
For :code:`RS`, the :code:`SET_FAILURES` key specifies
the maximum number of failures to tolerate within each redundancy set.
If not specified, this defaults to the value of :code:`SCR_SET_FAILURES`.

One checkpoint descriptor can be marked with the :code:`OUTPUT` key.
This indicates that the descriptor should be selected to store datasets
that the application flags with :code:`SCR_FLAG_OUTPUT`.
The :code:`OUTPUT` key is optional, and it defaults to 0.
If there is no descriptor with the :code:`OUTPUT` key defined
and if the dataset is also a checkpoint,
SCR chooses the checkpoint descriptor according to the normal policy.
Otherwise, if there is no descriptor with the :code:`OUTPUT` key defined
and if the dataset is not a checkpoint,
SCR uses the checkpoint descriptor having an interval of 1.

If one does not explicitly define a checkpoint descriptor,
the default SCR descriptor can be defined in pseudocode as::

  CKPT=0 INTERVAL=1 GROUP=$SCR_GROUP STORE=$SCR_CACHE_BASE TYPE=$SCR_COPY_TYPE SET_SIZE=$SCR_SET_SIZE BYPASS=$SCR_CACHE_BYPASS

If those parameters are not set otherwise, this defaults to the following::

  CKPT=0 INTERVAL=1 GROUP=NODE STORE=/dev/shm TYPE=XOR SET_SIZE=8 BYPASS=1

.. _sec-config-single-and-xor:

Example using SINGLE and XOR
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

On many systems, application failures are more common than node failures.
The :code:`SINGLE` redundancy scheme is sufficient to recover from application failures,
and it is much faster than other redundancy schemes like :code:`XOR`.
If there is room to store multiple checkpoints in cache,
one can configure SCR to use :code:`SINGLE` and :code:`XOR` in the same run.
For an application failure, SCR can restart the job from the most recent checkpoint,
but if a node fails, SCR can fallback to the most recent :code:`XOR` checkpoint.
The following entries configure SCR to encode every 10th checkpoint with :code:`XOR`
but use :code:`SINGLE` for all others::

  # instruct SCR to use the CKPT descriptors from the config file
  SCR_COPY_TYPE=FILE
  
  # enable datasets to be stored in cache
  SCR_CACHE_BYPASS=0

  # define distinct paths for SINGLE and XOR
  STORE=/dev/shm/single COUNT=1
  STORE=/dev/shm/xor    COUNT=1

  # save every 10th checkpoint using XOR
  # save all other checkpoints using SINGLE
  CKPT=0 INTERVAL=1  STORE=/dev/shm/single TYPE=SINGLE
  CKPT=1 INTERVAL=10 STORE=/dev/shm/xor    TYPE=XOR

This configures SCR to write all checkpoints within :code:`/dev/shm`,
but separate directories are used for :code:`SINGLE` and :code:`XOR`.
By defining distinct :code:`STORE` locations for each redundancy type,
SCR always deletes an older checkpoint of the same type before writing a new checkpoint.

.. _sec-variables:

SCR parameters
--------------

The table in this section specifies the full set of SCR configuration parameters.

.. %:code:`SCR_ENABLE` & 1 & Set to 0 to disable SCR at run time.
   %:code:`SCR_HOP_DISTANCE` & 1 & Set to a positive integer to specify the number of hops
   %taken to select a partner node for :code:`PARTNER`
   %or the number of hops between nodes of the same XOR set for :code:`XOR`.
   %In general, 1 will give the best performance, but a higher
   %value may enable SCR to recover from more severe failures which take down multiple
   %consecutive nodes (e.g., a power breaker which supplies a rack of consecutive nodes).

.. * - :code:`SCR_LOG_SYSLOG_PREFIX`
     - SCR
     - Prefix string to use in syslog messages.
   * - :code:`SCR_LOG_SYSLOG_FACILITY`
     - :code:`LOG_LOCAL7`
     - Facility value to be used in syslog messages.
   * - :code:`SCR_LOG_SYSLOG_LEVEL`
     - :code:`LOG_INFO`
     - Level value to be used in syslog messages.

.. The CRC support for data integrity needs to be refreshed after refactoring to components.
..   * - :code:`SCR_CRC_ON_COPY`
     - 0
     - Set to 1 to enable CRC32 checks when copying files during the redundancy scheme.
   * - :code:`SCR_CRC_ON_DELETE`
     - 0
     - Set to 1 to enable CRC32 checks when deleting files from cache.
   * - :code:`SCR_CRC_ON_FLUSH`
     - 1
     - Set to 0 to disable CRC32 checks during fetch and flush operations.

.. list-table:: SCR parameters
   :widths: 10 10 40
   :header-rows: 1

   * - Name
     - Default
     - Description
   * - :code:`SCR_DEBUG`
     - 0
     - Set to 1 or 2 for increasing verbosity levels of debug messages.
   * - :code:`SCR_CHECKPOINT_INTERVAL`
     - 0
     - Set to positive number of times :code:`SCR_Need_checkpoint` should be called before returning 1.
       This provides a simple way to set a periodic checkpoint frequency within an application.
   * - :code:`SCR_CHECKPOINT_SECONDS`
     - 0
     - Set to positive number of seconds to specify minimum time between consecutive checkpoints as guided by :code:`SCR_Need_checkpoint`.
   * - :code:`SCR_CHECKPOINT_OVERHEAD`
     - 0.0
     - Set to positive floating-point value to specify maximum percent overhead allowed for checkpointing operations as guided by :code:`SCR_Need_checkpoint`.
   * - :code:`SCR_CNTL_BASE`
     - :code:`/dev/shm`
     - Specify the default base directory SCR should use to store its runtime control metadata.  The control directory should be in fast, node-local storage like RAM disk.
   * - :code:`SCR_HALT_EXIT`
     - 0
     - Whether SCR should call :code:`exit()` when it detects an active halt condition.
       When enabled, SCR can exit the job during :code:`SCR_Init` and :code:`SCR_Complete_output` after each successful checkpoint.
       Set to 1 to enable.
   * - :code:`SCR_HALT_SECONDS`
     - 0 
     - Set to a positive integer to instruct SCR to halt the job
       if the remaining time in the current job allocation is less than the specified number of seconds.
   * - :code:`SCR_GROUP`
     - :code:`NODE`
     - Specify name of default failure group.
   * - :code:`SCR_COPY_TYPE`
     - :code:`XOR`
     - Set to one of: :code:`SINGLE`, :code:`PARTNER`, :code:`XOR`, :code:`RS`, or :code:`FILE`.
   * - :code:`SCR_CACHE_BASE`
     - :code:`/dev/shm`
     - Specify the default base directory SCR should use to cache datasets.
   * - :code:`SCR_CACHE_SIZE`
     - 1
     - Set to a non-negative integer to specify the maximum number of checkpoints SCR
       should keep in cache.  SCR will delete the oldest checkpoint from cache before
       saving another in order to keep the total count below this limit.
   * - :code:`SCR_CACHE_BYPASS`
     - 1
     - Specify bypass mode.  When enabled, data files are directly read from and written to the
       parallel file system, bypassing the cache.  Even in bypass mode, internal
       SCR metadata corresponding to the dataset is stored in cache.
       Set to 0 to direct SCR to store datasets in cache.
   * - :code:`SCR_CACHE_PURGE`
     - 0
     - Whether to delete all datasets from cache during :code:`SCR_Init`.
       Enabling this setting may be useful for test and development while integrating SCR in an application.
   * - :code:`SCR_SET_SIZE`
     - 8
     - Specify the minimum number of processes to include in an redundancy set.
       So long as there are sufficient failure groups, each redundancy set will be at least the minimum size.
       If not, redundancy sets will be as large as possible, but they may be smaller than the minimum size.
       Increasing this value can decrease the amount of storage required to cache the dataset.
       However, a higher value can require more time to rebuild lost files,
       and it increases the likelihood of encountering a catastrophic failure.
   * - :code:`SCR_SET_FAILURES`
     - 2
     - Specify the number of failures to tolerate in each set while using the RS scheme.
       Increasing this value enables one to tolerate more failures per set, but it increases
       redundancy storage and encoding costs.
   * - :code:`SCR_PREFIX`
     - $PWD
     - Specify the prefix directory on the parallel file system where datasets should be read from and written to.
   * - :code:`SCR_PREFIX_SIZE`
     - 0
     - Specify number of checkpoints to keep in the prefix directory.
       SCR deletes older checkpoints as new checkpoints are flushed to maintain a sliding window of the specified size.
       Set to 0 to keep all checkpoints.
       Checkpoints marked with :code:`SCR_FLAG_OUTPUT` are not deleted.
   * - :code:`SCR_PREFIX_PURGE`
     - 0
     - Set to 1 to delete all datasets from the prefix directory (both checkpoint and output) during :code:`SCR_Init`.
   * - :code:`SCR_CURRENT`
     - N/A
     - Name of checkpoint to mark as current and attempt to load during a new run during :code:`SCR_Init`.
   * - :code:`SCR_DISTRIBUTE`
     - 1
     - Set to 0 to disable cache rebuild during :code:`SCR_Init`.
   * - :code:`SCR_FETCH`
     - 1
     - Set to 0 to disable SCR from fetching files from the parallel file system during :code:`SCR_Init`.
   * - :code:`SCR_FETCH_BYPASS`
     - 0
     - Set to 1 to read files directly from the parallel file system during fetch.
   * - :code:`SCR_FETCH_WIDTH`
     - 256
     - Specify the number of processes that may read simultaneously from the parallel file system.
   * - :code:`SCR_FLUSH`
     - 10
     - Specify the number of checkpoints between periodic flushes to the parallel file system.  Set to 0 to disable periodic flushes.
   * - :code:`SCR_FLUSH_ASYNC`
     - 0
     - Set to 1 to enable asynchronous flush methods (if supported).
   * - :code:`SCR_FLUSH_POSTSTAGE`
     - 0
     - Set to 1 to finalize asynchronous flushes using the scr_poststage script,
       rather than in SCR_Finalize().  This can be used to start a checkpoint
       flush near the end of your job, and have it run "in the background" after
       your job finishes.  This is currently only supported by the IBM Burst
       Buffer API (BBAPI).   To use this, you need to make sure to specify
       `scr_poststage` as your 2nd-half post-stage script in bsub to
       finalize the transfers.  See `examples/test_scr_poststage` for a
       detailed example.
   * - :code:`SCR_FLUSH_TYPE`
     - :code:`SYNC`
     - Specify the flush transfer method.  Set to one of: :code:`SYNC`, :code:`PTHREAD`, :code:`BBAPI`, or :code:`DATAWARP`.
   * - :code:`SCR_FLUSH_WIDTH`
     - 256
     - Specify the number of processes that may write simultaneously to the parallel file system.
   * - :code:`SCR_FLUSH_ON_RESTART`
     - 0
     - Set to 1 to force SCR to flush datasets during restart.
       This is useful for applications that restart without using the SCR Restart API.
       Typically, one should also set :code:`SCR_FETCH=0` when enabling this option.
   * - :code:`SCR_GLOBAL_RESTART`
     - 0
     - Set to 1 to flush checkpoints to and restart from the prefix directory during :code:`SCR_Init`.
       This is needed by applications that use the SCR Restart API but require a global file system to restart,
       e.g., because multiple processes read the same file.
   * - :code:`SCR_RUNS`
     - 1
     - Specify the maximum number of times the :code:`scr_srun` command should attempt to run a job within an allocation.
       Set to -1 to specify an unlimited number of times.
   * - :code:`SCR_MIN_NODES`
     - N/A
     - Specify the minimum number of nodes required to run a job.
   * - :code:`SCR_EXCLUDE_NODES`
     - N/A
     - Specify a set of nodes, using SLURM node range syntax, which should be excluded from runs.
       This is useful to avoid particular problematic nodes.
       Nodes named in this list that are not part of a the current job allocation are silently ignored.
   * - :code:`SCR_LOG_ENABLE`
     - 0
     - Whether to enable any form of logging of SCR events.
   * - :code:`SCR_LOG_TXT_ENABLE`
     - 1
     - Whether to log SCR events to text file in prefix directory at :code:`$SCR_PREFIX/.scr/log`.
       :code:`SCR_LOG_ENABLE` must be set to 1 for this parameter to be active.
   * - :code:`SCR_LOG_SYSLOG_ENABLE`
     - 1
     - Whether to log SCR events to syslog.
       :code:`SCR_LOG_ENABLE` must be set to 1 for this parameter to be active.
   * - :code:`SCR_LOG_DB_ENABLE`
     - 0
     - Whether to log SCR events to MySQL database.
       :code:`SCR_LOG_ENABLE` must be set to 1 for this parameter to be active.
   * - :code:`SCR_LOG_DB_DEBUG`
     - 0
     - Whether to print MySQL statements as they are executed.
   * - :code:`SCR_LOG_DB_HOST`
     - N/A
     - Hostname of MySQL server
   * - :code:`SCR_LOG_DB_NAME`
     - N/A
     - Name of SCR MySQL database.
   * - :code:`SCR_LOG_DB_USER`
     - N/A
     - Username of SCR MySQL user.
   * - :code:`SCR_LOG_DB_PASS`
     - N/A
     - Password for SCR MySQL user.
   * - :code:`SCR_MPI_BUF_SIZE`
     - 131072
     - Specify the number of bytes to use for internal MPI send and receive buffers when computing redundancy data or rebuilding lost files.
   * - :code:`SCR_FILE_BUF_SIZE`
     - 1048576
     - Specify the number of bytes to use for internal buffers when copying files between the parallel file system and the cache.
   * - :code:`SCR_WATCHDOG_TIMEOUT`
     - N/A
     - Set to the expected time (seconds) for checkpoint writes to in-system storage (see :ref:`sec-hang`).
   * - :code:`SCR_WATCHDOG_TIMEOUT_PFS`
     - N/A
     - Set to the expected time (seconds) for checkpoint writes to the parallel file system (see :ref:`sec-hang`).
