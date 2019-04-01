.. _redundancy_descriptors:

Redundancy descriptors
======================

Overview
--------

A redundancy descriptor is a data structure that describes how a dataset
is cached. It tracks information such as the cache directory that is
used, the redundancy scheme that is applied, and the frequency with
which this combination should be used. The data structure also records
information on the group of processes that make up a redundancy set,
such as the number of processes in the set, as well as, a unique integer
that identifies the set, called the *group id*.

There is both a C struct and an equivalent specialized hash for storing
redundancy descriptors. The hash is primarily used to persist group
information across runs, such that the same process group can be
reconstructed in a later run (even if the user changes configuration
parameters between runs). These hashes are stored in filemap files. The
C struct is used within the library to cache additional runtime
information such as an MPI communicator for each group and the location
of certain MPI ranks.

During the run, the SCR library maintains an array of redundancy
descriptor structures in a global variable named ``scr_reddescs``. It
records the number of descriptors in this list in a variable named
``scr_nreddescs``. It builds this list during ``SCR_Init()`` using a
series of redundancy descriptor hashes defined in a third variable named
``scr_reddesc_hash``. The hashes in this variable are constructed while
processing SCR parameters.

Redundancy descriptor struct
----------------------------

Here is the definition for the C struct.

::

   typedef struct {
     int      enabled;        /* flag indicating whether this descriptor is active */
     int      index;          /* each descriptor is indexed starting from 0 */
     int      interval;       /* how often to apply this descriptor, pick largest such
                               * that interval evenly divides checkpoint id */
     int      store_index;    /* index into scr_storedesc for storage descriptor */
     int      group_index;    /* index into scr_groupdesc for failure group */
     char*    base;           /* base cache directory to use */
     char*    directory;      /* full directory base/dataset.id */
     int      copy_type;      /* redundancy scheme to apply */
     void*    copy_state;     /* pointer to extra state depending on copy type */
     MPI_Comm comm;           /* communicator holding procs for this scheme */
     int      groups;         /* number of redundancy sets */
     int      group_id;       /* unique id assigned to this redundancy set */
     int      ranks;          /* number of ranks in this set */
     int      my_rank;        /* caller's rank within its set */
   } scr_reddesc;

The ``enabled`` field is set to 0 (false) or 1 (true) to indicate
whether this particular redundancy descriptor may be used. Even though a
redundancy descriptor may be defined, it may be disabled. The ``index``
field records the index number of this redundancy descriptor. This
corresponds to the redundancy descriptor’s index in the ``scr_reddescs``
array. The ``interval`` field describes how often this redundancy
descriptor should be selected for different checkpoints. To choose a
redundancy descriptor to apply to a given checkpoint, SCR picks the
descriptor that has the largest interval value which evenly divides the
checkpoint id.

The ``store_index`` field tracks the index of the store descriptor
within the ``scr_storedescs`` array that describes the storage used with
this redundancy descriptor. The ``group_index`` field tracks the index
of the group descriptor within the ``scr_groupdescs`` array that
describes the group of processes likely to fail at the same time. The
redundancy scheme will protect against failures for this group using the
specified storage device.

The ``base`` field is a character array that records the cache base
directory that is used. The ``directory`` field is a character array
that records the directory in which the dataset subdirectory is created.
This path consists of the cache directory followed by the redundancy
descriptor index directory, such that one must only append the dataset
id to compute the full path of the corresponding dataset directory.

The ``copy_type`` field specifies the type of redundancy scheme that is
applied. It may be set to one of: ``SCR_COPY_NULL``,
``SCR_COPY_SINGLE``, ``SCR_COPY_PARTNER``, or ``SCR_COPY_XOR``. The
``copy_state`` field is a ``void*`` that points to any extra state that
is needed depending on the redundancy scheme.

The remaining fields describe the group of processes that make up the
redundancy set for a particular process. For a given redundancy
descriptor, the entire set of processes in the run is divided into
distinct groups, and each of these groups is assigned a unique integer
id called the group id. The set of group ids may not be contiguous. Each
process knows the total number of groups, which is recorded in the
``groups`` field, as well as, the id of the group the process is a
member of, which is recorded in the ``group_id`` field.

Since the processes within a group communicate frequently, SCR creates a
communicator for each group. The ``comm`` field is a handle to the MPI
communicator that defines the group the process is a member of. The
``my_rank`` and ``ranks`` fields cache the rank of the process in this
communicator and the number of processes in this communicator,
respectively.

If the redundancy scheme requires additional information to be kept in
the redundancy descriptor, it allocates additional memory and records a
pointer to it via the ``copy_state`` pointer.

Extra state for PARTNER
~~~~~~~~~~~~~~~~~~~~~~~

The ``SCR_COPY_PARTNER`` scheme allocates the following structure:

::

   typedef struct {
     int       lhs_rank;       /* rank which is one less (with wrap to highest) within set */
     int       lhs_rank_world; /* rank of lhs process in comm world */
     char*     lhs_hostname;   /* hostname of lhs process */
     int       rhs_rank;       /* rank which is one more (with wrap to lowest) within set */
     int       rhs_rank_world; /* rank of rhs process in comm world */
     char*     rhs_hostname;   /* hostname of rhs process */
   } scr_reddesc_partner;

For ``SCR_COPY_PARTNER``, the processes within a group form a logical
ring, ordered by their rank in the group. Each process has a left and
right neighbor in this ring. The left neighbor is the process whose rank
is one less than the current process, and the right neighbor is the
process whose rank is one more. The last process in the group wraps back
around to the first. SCR caches information about the ranks to the left
and right of a process. The ``lhs_rank``, ``lhs_rank_world``, and
``lhs_hostname`` fields describe the rank to the left of the process,
and the ``rhs_rank``, ``rhs_rank_world``, and ``rhs_hostname`` fields
describe the rank to the right. The ``lhs_rank`` and ``rhs_rank`` fields
record the ranks of the neighbor processes in ``comm``. The
``lhs_rank_world`` and ``rhs_rank_world`` fields record the ranks of the
neighbor processes in ``scr_comm_world``. Finally, the ``lhs_hostname``
and ``rhs_hostname`` fields record the hostnames where those processes
are running.

Extra state for XOR
~~~~~~~~~~~~~~~~~~~

The ``SCR_COPY_XOR`` scheme allocates the following structure:

::

   typedef struct {
     scr_hash* group_map;      /* hash that maps group rank to world rank */
     int       lhs_rank;       /* rank which is one less (with wrap to highest) within set */
     int       lhs_rank_world; /* rank of lhs process in comm world */
     char*     lhs_hostname;   /* hostname of lhs process */
     int       rhs_rank;       /* rank which is one more (with wrap to lowest) within set */
     int       rhs_rank_world; /* rank of rhs process in comm world */
     char*     rhs_hostname;   /* hostname of rhs process */
   } scr_reddesc_xor;

The fields here are similar to the fields of ``SCR_COPY_PARTNER`` with
the exception of an additional ``group_map`` field, which records a hash
that maps a group rank to its rank in ``MPI_COMM_WORLD``.

Example redundancy descriptor hash
----------------------------------

Each redundancy descriptor can be stored in a hash. Here is an example
redundancy descriptor hash.

::

   ENABLED
     1
   INDEX
     0
   INTERVAL
     1
   BASE
     /tmp
   DIRECTORY
     /tmp/user1/scr.1145655/index.0
   TYPE
     XOR
   HOP_DISTANCE
     1
   SET_SIZE
     8
   GROUPS
     1
   GROUP_ID
     0
   GROUP_SIZE
     4
   GROUP_RANK
     0

Most field names in the hash match field names in the C struct, and the
meanings are the same. The one exception is ``GROUP_RANK``, which
corresponds to ``my_rank`` in the struct. Note that not all fields from
the C struct are recorded in the hash. At runtime, it’s possible to
reconstruct data for the missing struct fields using data from the hash.
In particular, one may recreate the group communicator by calling
``MPI_Comm_split()`` on ``scr_comm_world`` specifying the ``GROUP_ID``
value as the color and specifying the ``GROUP_RANK`` value as the key.
After recreating the group communicator, one may easily find info for
the left and right neighbors.

Example redundancy descriptor configuration file entries
--------------------------------------------------------

SCR must be configured with redundancy schemes. By default, SCR protects
against single compute node failures using ``XOR``, and it caches one
checkpoint in ``/tmp``. To specify something different, edit a
configuration file to include checkpoint descriptors. Checkpoint
descriptors look like the following.

::

   # instruct SCR to use the CKPT descriptors from the config file
   SCR_COPY_TYPE=FILE

   # the following instructs SCR to run with three checkpoint configurations:
   # - save every 8th checkpoint to /ssd using the PARTNER scheme
   # - save every 4th checkpoint (not divisible by 8) to /ssd using XOR with
   #   a set size of 8
   # - save all other checkpoints (not divisible by 4 or 8) to /tmp using XOR with
   #   a set size of 16
   CKPT=0 INTERVAL=1 GROUP=NODE   STORE=/tmp TYPE=XOR     SET_SIZE=16
   CKPT=1 INTERVAL=4 GROUP=NODE   STORE=/ssd TYPE=XOR     SET_SIZE=8
   CKPT=2 INTERVAL=8 GROUP=SWITCH STORE=/ssd TYPE=PARTNER

First, one must set the ``SCR_COPY_TYPE`` parameter to “``FILE``”.
Otherwise, an implied checkpoint descriptor is constructed using various
SCR parameters including ``SCR_GROUP``, ``SCR_CACHE_BASE``,
``SCR_COPY_TYPE``, and ``SCR_SET_SIZE``.

Checkpoint descriptor entries are identified by a leading ``CKPT`` key.
The values of the ``CKPT`` keys must be numbered sequentially starting
from 0. The ``INTERVAL`` key specifies how often a checkpoint is to be
applied. For each checkpoint, SCR selects the descriptor having the
largest interval value that evenly divides the internal SCR checkpoint
iteration number. It is necessary that one descriptor has an interval of
1. This key is optional, and it defaults to 1 if not specified. The
``GROUP`` key lists the failure group, i.e., the name of the group of
processes likely to fail. This key is optional, and it defaults to the
value of the ``SCR_GROUP`` parameter if not specified. The ``STORE`` key
specifies the directory in which to cache the checkpoint. This key is
optional, and it defaults to the value of the ``SCR_CACHE_BASE``
parameter if not specified. The ``TYPE`` key identifies the redundancy
scheme to be applied. This key is optional, and it defaults to the value
of the ``SCR_COPY_TYPE`` parameter if not specified.

Other keys may exist depending on the selected redundancy scheme. For
``XOR`` schemes, the ``SET_SIZE`` key specifies the minimum number of
processes to include in each ``XOR`` set.

Common functions
----------------

This section describes some of the most common redundancy descriptor
functions. The implementation can be found in ``scr.c``.

Initializing and freeing redundancy descriptors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Initialize a redundancy descriptor structure (clear its fields).

::

     struct scr_reddesc desc;
     scr_reddesc_init(&desc)

Free memory associated with a redundancy descriptor.

::

     scr_reddesc_free(&desc)

Redundancy descriptor array
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Allocate and fill in ``scr_reddescs`` array using redundancy descriptor
hashes provided in ``scr_reddesc_hash``.

::

     scr_reddescs_create()

Free the list of redundancy descriptors.

::

     scr_reddescs_free()

Select a redundancy descriptor for a specified checkpoint id from among
the ``ndescs`` descriptors in the array of descriptor structs pointed to
by ``descs``.

::

     struct scr_reddesc* desc = scr_reddesc_for_checkpoint(ckpt, ndescs, descs)

Converting between structs and hashes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Convert a redundancy descriptor struct to its equivalent hash.

::

     scr_reddesc_store_to_hash(desc, hash)

This function clears any entries in the specified hash before setting
fields according to the struct.

Given a redundancy descriptor hash, build and fill in the fields for its
equivalent redundancy descriptor struct.

::

     scr_reddesc_create_from_hash(desc, index, hash)

This function creates a communicator for the redundancy group and fills
in neighbor information relative to the calling process. Note that this
call is collective over ``scr_comm_world``, because it creates a
communicator. The index value specified in the call is overridden if an
index field is set in the hash.

Interacting with filemaps
~~~~~~~~~~~~~~~~~~~~~~~~~

Redundancy descriptor hashes are cached in filemaps. There are functions
to set, get, and unset a redundancy descriptor hash in a filemap for a
given dataset id and rank id
(Section :ref:`Filemap redundancy descriptors <filemap_redundancy_descriptors>`).
There are additional functions to extract info from a redundancy
descriptor hash that is stored in a filemap.

For a given dataset id and rank id, return the base directory associated
with the redundancy descriptor stored in the filemap.

::

     char* basedir = scr_reddesc_base_from_filemap(map, dset, rank)

For a given dataset id and rank id, return the path associated with the
redundancy descriptor stored in the filemap in which dataset directories
are to be created.

::

     char* dir = scr_reddesc_dir_from_filemap(map, dset, rank)

For a given dataset id and rank id, fill in the specified redundancy
descriptor struct using the redundancy descriptor stored in the filemap.

::

     scr_reddesc_create_from_filemap(map, dset, rank, desc)

Note that this call is collective over ``scr_comm_world``, because it
creates a communicator.
