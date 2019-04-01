.. _group_descriptors:

Group descriptors
=================

Overview
--------

A group descriptor is a data structure that describes a group of
processes. Each group is given a name, which is used as a key to refer
to the group. For each group name, a process belongs to at most one
group, which is a subset of all processes in the job.

There are two pre-defined groups: ``WORLD`` which contains all processes
in ``MPI_COMM_WORLD`` and ``NODE`` which contains all processes on the
same node. SCR determines which processes are on the same node by
splitting processes into groups that have the same value for
``scr_my_hostname``, which is set by calling ``scr_env_hostname()``.

Additional groups may be defined via entries in the system or user
configuration files. It is necessary to define additional groups when
failure modes or storage devices span multiple compute nodes. For
example if network switch failures are common, then one could define a
group to specify which nodes share a network switch to enable SCR to
protect against such failures.

The group descriptor is a C struct. During the run, the SCR library
maintains an array of group descriptor structures in a global variable
named ``scr_groupdescs``. It records the number of descriptors in this
list in a variable named ``scr_ngroupdescs``. It builds this list during
``SCR_Init()`` by calling ``scr_groupdescs_create()`` which constructs
the list from a third variable called ``scr_groupdescs_hash``. This hash
variable is initialized from entries in the configuration files while
processing SCR parameters. The group structures are freed in
``SCR_Finalize()`` by calling ``scr_groupdescs_free()``.

Group descriptor struct
-----------------------

Here is the definition for the C struct.

::

   typedef struct {
     int      enabled;      /* flag indicating whether this descriptor is active */
     int      index;        /* each descriptor is indexed starting from 0 */
     char*    name;         /* name of group */
     MPI_Comm comm;         /* communicator of processes in same group */
     int      rank;         /* local rank of process in communicator */
     int      ranks;        /* number of ranks in communicator */
   } scr_groupdesc;

The ``enabled`` field is set to 0 (false) or 1 (true) to indicate
whether this particular group descriptor may be used. Even though a
group descriptor may be defined, it may be disabled. The ``index`` field
records the index within the ``scr_groupdescs`` array. The ``name``
field is a copy of the group name. The ``comm`` field is a handle to the
MPI communicator that defines the group the process is a member of. The
``rank`` and ``ranks`` fields cache the rank of the process in this
communicator and the number of processes in this communicator,
respectively.

Example group descriptor configuration file entries
---------------------------------------------------

Here are some examples of configuration file entries to define new
groups.

::

   GROUPS=zin1  POWER=psu1  SWITCH=0
   GROUPS=zin2  POWER=psu1  SWITCH=1
   GROUPS=zin3  POWER=psu2  SWITCH=0
   GROUPS=zin4  POWER=psu2  SWITCH=1

Group descriptor entries are identified by a leading ``GROUPS`` key.
Each line corresponds to a single compute node, where the hostname is
the value of the ``GROUPS`` key. There must be one line for every
compute node in the allocation. It is recommended to specify groups in
the system configuration file.

The remaining values on the line specify a set of group name / value
pairs. The group name is the string to be referenced by store and
checkpoint descriptors. The value can be an arbitrary character string.
The only requirement is that for a given group name, nodes that form a
group must specify identical values.

In the above example, there are four compute nodes: zin1, zin2, zin3,
and zin4. There are two groups defined: ``POWER`` and ``SWITCH``. Nodes
zin1 and zin2 belong to the same ``POWER`` group, as do nodes zin3 and
zin4. For the ``SWITCH`` group, nodes zin1 and zin3 belong to the same
group, as do nodes zin2 and zin4.

Common functions
----------------

This section describes some of the most common group descriptor
functions. These functions are defined in ``scr_groupdesc.h`` and
implemented in ``scr_groupdesc.c``.

Creating and freeing the group descriptors array
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To initialize the ``scr_groupdescs`` and ``scr_ngroupdescs`` variables
from the ``scr_groupdescs_hash`` variable:

::

   scr_groupdescs_create();

Free group descriptors array.

::

   scr_groupdescs_free();

Lookup group descriptor by name
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To lookup a group descriptor by name.

::

   scr_groupdesc* group = scr_groupdescs_from_name(name);

This returns NULL if the specified group name is not defined. There is
also a function to return the index of a group within
``scr_groupdescs``.

::

   int index = scr_groupdescs_index_from_name(name);

This returns an index value in the range
:math:`[0, \texttt{scr\_ngroupdescs})` if the specified group name is
defined and it returns -1 otherwise.
