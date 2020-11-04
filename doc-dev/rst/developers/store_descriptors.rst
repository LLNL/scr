.. _store_descriptors:

Store descriptors
=================

Overview
--------

A store descriptor is a data structure that describes a class of
storage. Each store is given a name, which is used as a key to refer to
the storage device.

All storage devices available to SCR must be specified via entries in
the system or user configuration files. These entries specify which
nodes can access the device, the capacity of the device, and other
capabilities such as whether it supports the creation of directories.

The store descriptor is a C struct. During the run, the SCR library
maintains an array of store descriptor structures in a global variable
named ``scr_storedescs``. It records the number of descriptors in this
list in a variable named ``scr_nstoredescs``. It builds this list during
``SCR_Init()`` by calling ``scr_storedescs_create()`` which constructs
the list from a third variable called ``scr_storedescs_hash``. This hash
variable is initialized from entries in the configuration files while
processing SCR parameters. The store structures are freed in
``SCR_Finalize()`` by calling ``scr_storedescs_free()``.

Store descriptor struct
-----------------------

Here is the definition for the C struct.

::

   typedef struct {
     int      enabled;   /* flag indicating whether this descriptor is active */
     int      index;     /* each descriptor is indexed starting from 0 */
     char*    name;      /* name of store */
     int      max_count; /* maximum number of datasets to be stored in device */
     int      can_mkdir; /* flag indicating whether mkdir/rmdir work */
     char*    type;      /* AXL xfer type string (bbapi, sync, pthread, etc..) */
     char*    view;      /* indicates whether store is node-local or global */
     MPI_Comm comm;      /* communicator of processes that can access storage */
     int      rank;      /* local rank of process in communicator */
     int      ranks;     /* number of ranks in communicator */
   } scr_storedesc;

The ``enabled`` field is set to 0 (false) or 1 (true) to indicate
whether this particular store descriptor may be used. Even though a
store descriptor may be defined, it may be disabled. The ``index`` field
records the index within the ``scr_storedescs`` array. The ``name``
field is a copy of the store name. The ``comm`` field is a handle to the
MPI communicator that defines the group the processes that share access
to the storage device that the local process uses. The ``rank`` and
``ranks`` fields cache the rank of the process in this communicator and
the number of processes in this communicator, respectively.  ``type`` is
the name of the AXL (https://github.com/ECP-VeloC/AXL) transfer type used
internally to copy the files into the storage descriptor.  Some transfer
types are:

sync:       A basic synchronous file copy
pthread:    Multi-threaded file copy
bbapi:      Use the IBM Burst Buffer API (if available)
dw:         Use the Cray DataWarp API (if available)


Example store descriptor configuration file entries
---------------------------------------------------

SCR must know about the storage devices available on a system. SCR
requires that all processes be able to access the prefix directory, and
it assumes that ``/tmp`` is storage local to each compute node.
Additional storage can be described in configuration files with entries
like the following.

::

   STORE=/tmp          GROUP=NODE   COUNT=1
   STORE=/ssd          GROUP=NODE   COUNT=3  TYPE=bbapi
   STORE=/dev/persist  GROUP=NODE   COUNT=1  ENABLED=1  MKDIR=0
   STORE=/p/lscratcha  GROUP=WORLD  TYPE=pthread

Store descriptor entries are identified by a leading ``STORE`` key. Each
line corresponds to a class of storage devices. The value associated
with the ``STORE`` key is the directory prefix of the storage device.
This directory prefix also serves as the name of the store descriptor.
All compute nodes must be able to access their respective storage device
via the specified directory prefix.

The remaining values on the line specify properties of the storage
class. The ``GROUP`` key specifies the group of processes that share a
device. Its value must specify a group name. The ``COUNT`` key specifies
the maximum number of checkpoints that can be kept in the associated
storage. The user should be careful to set this appropriately depending
on the storage capacity and the application checkpoint size. The
``COUNT`` key is optional, and it defaults to the value of the
``SCR_CACHE_SIZE`` parameter if not specified. The ``ENABLED`` key
enables (1) or disables (0) the store descriptor. This key is optional,
and it defaults to 1 if not specified. The ``MKDIR`` key specifies
whether the device supports the creation of directories (1) or not (0).
This key is optional, and it defaults to 1 if not specified.  ``TYPE``
is the AXL transfer type used to copy files into the store descriptor.
Values for `TYPE` include:

sync:          A basic synchronous file copy
pthread:       Multi-threaded file copy
bbapi:         Use the IBM Burst Buffer API (if available)
dw:            Use the Cray DataWarp API (if available)

``TYPE`` is optional, and will default to pthread if not specified.

In the above example, there are four storage devices specified:
``/tmp``, ``/ssd``, ``/dev/persist``, and ``/p/lscratcha``. The storage
at ``/tmp``, ``/ssd``, and ``/dev/persist`` specify the ``NODE`` group,
which means that they are node-local storage. Processes on the same
compute node access the same device. The storage at ``/p/lscratcha``
specifies the ``WORLD`` group, which means that all processes in the job
can access the device. In other words, it is a globally accessible file
system.

Common functions
----------------

This section describes some of the most common store descriptor
functions. These functions are defined in ``scr_storedesc.h`` and
implemented in ``scr_storedesc.c``.

Creating and freeing the store descriptors array
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To initialize the ``scr_storedescs`` and ``scr_nstoredescs`` variables
from the ``scr_storedescs_hash`` variable:

::

   scr_storedescs_create();

Free store descriptors array.

::

   scr_storedescs_free();

Lookup store descriptor by name
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To lookup a store descriptor by name.

::

   int index = scr_storedescs_index_from_name(name);

This returns an index value in the range
:math:`[0, \texttt{scr\_nstoredescs})` if the specified store name is
defined and it returns -1 otherwise.

Create and delete directories on storage device
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These functions are collective over the group of processes that share
access to the same storage device. To create a directory on a storage
device.

::

   int scr_storedesc_dir_create(const scr_storedesc* s, const char* dir);

To delete a directory.

::

   int scr_storedesc_dir_delete(const scr_storedesc* s, const char* dir);
