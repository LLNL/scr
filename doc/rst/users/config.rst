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
* System configuration file,
* Compile-time constants.

Some parameters, such as the location of the control directory,
cannot be specified by the user.
Such parameters must be either set in the system configuration file
or hard-coded into SCR as compile-time constants.

To find a user configuration file,
SCR looks for a file named :code:`.scrconf` in the prefix directory (note the leading dot).
Alternatively, one may specify the name and location of the user configuration file
by setting the :code:`SCR_CONF_FILE` environment variable at run time, e.g.,::

  export SCR_CONF_FILE=~/myscr.conf

The location of the system configuration file is hard-coded into SCR at build time.
This defaults to :code:`/etc/scr/scr.conf`.
One may set this using the :code:`SCR_CONFIG_FILE` option with cmake, e.g.,::

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

.. _sec-descriptors:

Group, store, and checkpoint descriptors
----------------------------------------

SCR must have information about process groups,
storage devices, and redundancy schemes.
The defaults provide reasonable settings for Linux clusters,
but one can define custom settings via group, store,
and checkpoint descriptors in configuration files.

SCR must know which processes are likely to fail
at the same time (failure groups) and which processes access a common
storage device (storage groups).
By default, SCR creates a group of all processes in the job called :code:`WORLD`
and another group of all processes on the same compute node called :code:`NODE`.
If more groups are needed, they can be defined in configuration files
with entries like the following::

  GROUPS=host1  POWER=psu1  SWITCH=0
  GROUPS=host2  POWER=psu1  SWITCH=1
  GROUPS=host3  POWER=psu2  SWITCH=0
  GROUPS=host4  POWER=psu2  SWITCH=1

Group descriptor entries are identified by a leading :code:`GROUPS` key.
Each line corresponds to a single compute node,
where the hostname is the value of the :code:`GROUPS` key.
There must be one line for every compute node in the allocation.
It is recommended to specify groups in the system configuration file.

The remaining values on the line specify a set of group name / value pairs.
The group name is the string to be referenced by store and checkpoint descriptors.
The value can be an arbitrary character string.
The only requirement is that for a given group name,
nodes that form a group must provide identical strings the value.

In the above example, there are four compute nodes: host1, host2, host3, and host4.
There are two groups defined: :code:`POWER` and :code:`SWITCH`.
Nodes host1 and host2 belong to the same :code:`POWER` group, as do nodes host3 and host4.
For the :code:`SWITCH` group, nodes host1 and host3 belong to the same group,
as do nodes host2 and host4.

In addition to groups,
SCR must know about the storage devices available on a system.
SCR requires that all processes be able to access the prefix directory,
and it assumes that :code:`/tmp` is storage local to each compute node.
Additional storage can be described in configuration files
with entries like the following::

  STORE=/tmp          GROUP=NODE   COUNT=1
  STORE=/ssd          GROUP=NODE   COUNT=3
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
The :code:`COUNT` key specifies the maximum number of checkpoints
that can be kept in the associated storage.
The user should be careful to set this appropriately
depending on the storage capacity and the application checkpoint size.
The :code:`COUNT` key is optional, and it defaults to the value
of the :code:`SCR_CACHE_SIZE` parameter if not specified.
The :code:`ENABLED` key enables (1) or disables (0) the store descriptor.
This key is optional, and it defaults to 1 if not specified.
The :code:`MKDIR` key specifies whether the device supports the
creation of directories (1) or not (0).
This key is optional, and it defaults to 1 if not specified.

In the above example, there are four storage devices specified:
:code:`/tmp`, :code:`/ssd`, :code:`/dev/persist`, and :code:`/p/lscratcha`.
The storage at :code:`/tmp`, :code:`/ssd`, and :code:`/dev/persist`
specify the :code:`NODE` group, which means that they are node-local storage.
Processes on the same compute node access the same device.
The storage at :code:`/p/lscratcha` specifies the :code:`WORLD` group,
which means that all processes in the job can access the device.
In other words, it is a globally accessible file system.

Finally, SCR must be configured with redundancy schemes.
By default, SCR protects against single compute node failures using :code:`XOR`,
and it caches one checkpoint in :code:`/tmp`.
To specify something different,
edit a configuration file to include checkpoint and output descriptors.
These descriptors look like the following::

  # instruct SCR to use the CKPT descriptors from the config file
  SCR_COPY_TYPE=FILE
  
  # the following instructs SCR to run with three checkpoint configurations:
  # - save every 8th checkpoint to /ssd using the PARTNER scheme
  # - save every 4th checkpoint (not divisible by 8) to /ssd using XOR with
  #   a set size of 8
  # - save all other checkpoints (not divisible by 4 or 8) to /tmp using XOR with
  #   a set size of 16
  CKPT=0 INTERVAL=1 GROUP=NODE   STORE=/tmp TYPE=XOR     SET_SIZE=16
  CKPT=1 INTERVAL=4 GROUP=NODE   STORE=/ssd TYPE=XOR     SET_SIZE=8  OUTPUT=1
  CKPT=2 INTERVAL=8 GROUP=SWITCH STORE=/ssd TYPE=PARTNER
  
  CKPT=0 INTERVAL=1 GROUP=NODE   STORE=/tmp TYPE=XOR     SET_SIZE=16

First, one must set the :code:`SCR_COPY_TYPE` parameter to ":code:`FILE`".
Otherwise, an implied checkpoint descriptor is constructed using various SCR parameters
including :code:`SCR_GROUP`, :code:`SCR_CACHE_BASE`,
:code:`SCR_COPY_TYPE`, and :code:`SCR_SET_SIZE`.

Checkpoint descriptor entries are identified by a leading :code:`CKPT` key.
The values of the :code:`CKPT` keys must be numbered sequentially starting from 0.
The :code:`INTERVAL` key specifies how often a descriptor is to be applied.
For each checkpoint,
SCR selects the descriptor having the largest interval value that evenly
divides the internal SCR checkpoint iteration number.
It is necessary that one descriptor has an interval of 1.
This key is optional, and it defaults to 1 if not specified.
The :code:`GROUP` key lists the failure group,
i.e., the name of the group of processes likely to fail.
This key is optional, and it defaults to the value of the
:code:`SCR_GROUP` parameter if not specified.
The :code:`STORE` key specifies the directory in which to cache the checkpoint.
This key is optional, and it defaults to the value of the
:code:`SCR_CACHE_BASE` parameter if not specified.
The :code:`TYPE` key identifies the redundancy scheme to be applied.
This key is optional, and it defaults to the value of the
:code:`SCR_COPY_TYPE` parameter if not specified.

Other keys may exist depending on the selected redundancy scheme.
For :code:`XOR` schemes, the :code:`SET_SIZE` key specifies
the minimum number of processes to include in each :code:`XOR` set.

One checkpoint descriptor can be marked with the :code:`OUTPUT` key.
This indicates that the descriptor should be selected to store datasets
that the application flags with :code:`SCR_FLAG_OUTPUT`.
The :code:`OUTPUT` key is optional, and it defaults to 0.
If there is no descriptor with the :code:`OUTPUT` key defined
and if the dataset is also a checkpoint,
SCR will choose the checkpoint descriptor according to the normal policy.
Otherwise, if there is no descriptor with the :code:`OUTPUT` key defined
and if the dataset is not a checkpoint,
SCR will use the checkpoint descriptor having interval of 1.

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

.. list-table:: SCR parameters
   :widths: 10 10 40
   :header-rows: 1

   * - Name
     - Default
     - Description
   * - :code:`SCR_HALT_SECONDS`
     - 0 
     - Set to a positive integer to instruct SCR to halt the job after completing
       a successful checkpoint if the remaining time in the current job allocation
       is less than the specified number of seconds.
   * - :code:`SCR_HALT_ENABLED`
     - 1
     - Whether SCR should halt a job by calling :code:`exit()`. Set to 0 to disable
       in which case the application is responsible for stopping.
   * - :code:`SCR_GROUP`
     - :code:`NODE`
     - Specify name of failure group.
   * - :code:`SCR_COPY_TYPE`
     - :code:`XOR`
     - Set to one of: :code:`SINGLE`, :code:`PARTNER`, :code:`XOR`, or :code:`FILE`.
   * - :code:`SCR_CACHE_BASE`
     - :code:`/tmp`
     - Specify the base directory SCR should use to cachecheckpoints.
   * - :code:`SCR_CACHE_SIZE`
     - 1
     - Set to a non-negative integer to specify the maximum number of checkpoints SCR
       should keep in cache.  SCR will delete the oldest checkpoint from cache before
       saving another in order to keep the total count below this limit.
   * - :code:`SCR_SET_SIZE`
     - 8
     - Specify the minimum number of processes to include in an XOR set.
       Increasing this value decreases the amount of storage required to cache the checkpoint data.
       However, higher values have an increased likelihood of encountering a catastrophic error.
       Higher values may also require more time to reconstruct lost files from redundancy data.
   * - :code:`SCR_PREFIX`
     - $PWD
     - Specify the prefix directory on the parallel file system where checkpoints should be read from and written to.
   * - :code:`SCR_CHECKPOINT_SECONDS`
     - 0
     - Set to positive number of seconds to specify minimum time between consecutive checkpoints as guided by :code:`SCR_Need_checkpoint`.
   * - :code:`SCR_CHECKPOINT_OVERHEAD`
     - 0.0
     - Set to positive percentage to specify maximum overhead allowed for checkpointing operations as guided by :code:`SCR_Need_checkpoint`.
   * - :code:`SCR_DISTRIBUTE`
     - 1
     - Set to 0 to disable file distribution during :code:`SCR_Init`.
   * - :code:`SCR_FETCH`
     - 1
     - Set to 0 to disable SCR from fetching files from the parallel file system during :code:`SCR_Init`.
   * - :code:`SCR_FETCH_WIDTH`
     - 256
     - Specify the number of processes that may read simultaneously from the parallel file system.
   * - :code:`SCR_FLUSH`
     - 10
     - Specify the number of checkpoints between periodic SCR flushes to the parallel file system.  Set to 0 to disable periodic flushes.
   * - :code:`SCR_FLUSH_ASYNC`
     - 0
     - Set to 1 to enable asynchronous flush methods (if supported).
   * - :code:`SCR_FLUSH_WIDTH`
     - 256
     - Specify the number of processes that may write simultaneously to the parallel file system.
   * - :code:`SCR_FLUSH_ON_RESTART`
     - 0
     - Set to 1 to force SCR to flush a checkpoint during restart.  This is useful for codes that must restart from the parallel file system.
   * - :code:`SCR_PRESERVE_DIRECTORIES`
     - 1
     - Whether SCR should preserve the application directory structure in prefix directory in flush and scavenge operations.  Set to 0 to rely on SCR-defined directory layouts.
   * - :code:`SCR_RUNS`
     - 1
     - Specify the maximum number of times the :code:`scr_srun` command should attempt to run a job within an allocation.  Set to -1 to specify an unlimited number of times.
   * - :code:`SCR_MIN_NODES`
     - N/A
     - Specify the minimum number of nodes required to run a job.
   * - :code:`SCR_EXCLUDE_NODES`
     - N/A
     - Specify a set of nodes, using SLURM node range syntax, which should be excluded from runs.  This is useful to avoid particular nodes while waiting for them to be fixed by system administrators.  Nodes in this list which are not in the current allocation are silently ignored.
   * - :code:`SCR_MPI_BUF_SIZE`
     - 131072
     - Specify the number of bytes to use for internal MPI send and receive buffers when computing redundancy data or rebuilding lost files.
   * - :code:`SCR_FILE_BUF_SIZE`
     - 1048576
     - Specify the number of bytes to use for internal buffers when copying files between the parallel file system and the cache.
   * - :code:`SCR_CRC_ON_COPY`
     - 0
     - Set to 1 to enable CRC32 checks when copying files during the redundancy scheme.
   * - :code:`SCR_CRC_ON_DELETE`
     - 0
     - Set to 1 to enable CRC32 checks when deleting files from cache.
   * - :code:`SCR_CRC_ON_FLUSH`
     - 1
     - Set to 0 to disable CRC32 checks during fetch and flush operations.
   * - :code:`SCR_DEBUG`
     - 0
     - Set to 1 or 2 for increasing verbosity levels of debug messages.
   * - :code:`SCR_WATCHDOG_TIMEOUT`
     - N/A
     - Set to the expected time (seconds) for checkpoint writes to in-system storage (See Section :ref:`sec-hang`).
   * - :code:`SCR_WATCHDOG_TIMEOUT_PFS`
     - N/A
     - Set to the expected time (seconds) for checkpoint writes to the parallel file system (See Section :ref:`sec-hang`).
