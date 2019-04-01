Redundancy schemes
==================

SINGLE
------

With the ``SINGLE`` redundancy scheme, SCR keeps a single copy of each
dataset file. It tracks meta data on application files, but no
redundancy data is stored. The communicator in the redundancy descriptor
is a duplicate of ``MPI_COMM_SELF``. During a restart, files are
distributed according to the current process mapping, but if a failure
leads to a loss of files, affected datasets are simply deleted from the
cache.

Although the lack of redundancy prevents ``SINGLE`` from being useful in
cases where access to storage is lost, in practice many failures are due
to application-level software which do not impact storage accessibility.
This scheme is also useful when writing to highly reliable storage.

PARTNER
-------

TODO: This scheme assumes node-local storage.

With ``PARTNER`` a full copy of every dataset file is made. The partner
process is always selected from a different failure group, so that it’s
unlikely for a process and its partner to fail at the same time.

When creating the redundancy descriptor, SCR splits ``scr_comm_world``
into subcommunicators each of which contains at most one process from
each failure group. Within this communicator, each process picks the
process whose rank is one more than its own (right-hand side) to send
its copies, and it stores copies of the files from the process whose
rank is one less (left-hand side). Processes at the end of the
communicator wrap around to find partners. The hostname, the rank within
the redundancy communicator, and the global rank of the left and right
neighbors are stored in the ``copy_state`` field of the redundancy
descriptor. This is all implemented in ``scr_reddesc_create()`` and
``scr_reddesc_create_partner()`` in ``scr_reddesc.c``.

When applying the redundancy scheme, each process sends its files to its
right neighbor. The meta data for each file is transferred and stored,
as well as, the redundancy descriptor hash for the process. Each process
writes the copies to the same dataset directory in which it wrote its
own original files. Note that if a process and its partner share access
to the same storage, then this scheme should not be used.

Each process also records the name of the node for which it is serving
as the partner. This information is used during a scavenge in order to
target the partner node for a copy if the source node has failed. This
is a useful optimization when the cache is in node-local storage.

During the distribute phase of a restart, a process may obtain its files
from either the original copy or the partner copy. If neither is
available, the distribute fails and the dataset is deleted from cache.
If the distribute phase succeeds, the ``PARTNER`` scheme is immediately
applied again to restore the redundancy.

During the first round of a scavenge, only original files are copied
from cache to the parallel file system. If the scavenge fails to copy
data from some nodes, the second round attempts to target just the
relevant partner nodes which it identified from the partner key recorded
in the filemap. This optimization avoids unnecessarily copying every
files twice, the original plus its copy.
