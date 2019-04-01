Containers
==========

NOTE: This feature is experimental and not yet complete, so it it not
documented in the user guide.

SCR requires checkpoint data to be stored primarily as a file per
process. However, writing a large number of files is inefficient or
difficult to manage on some file systems. To alleviate this problem, SCR
provides an abstraction called “containers”. When writing data to or
reading data from the prefix directory, SCR combines multiple
application files into a container. Containers are disabled by default.
To enable them, set the ``SCR_USE_CONTAINERS`` parameter to 1.

During a flush, SCR identifies the containers and the offsets within
those containers where each file should be stored. SCR records the
file-to-container mapping in the rank2file map, which it later
references to extract files during the fetch operation.

A container has a maximum size, which is determined by the
``SCR_CONTAINER_SIZE`` parameter. This parameter defaults to 100GB.
Application file data is packed sequentially within a container until
the container is full, and then the remaining data spills over to the
next container. The total number of containers required depends on the
total number of bytes in the dataset and the container size. A container
file name is of the form ``ctr.<id>.scr``, where ``<id>`` is the
container id which counts up from 0. All containers are written to the
dataset directory within the prefix directory.

SCR combines files in an order such that all files on the same node are
grouped sequentially. This limits the number of files that each compute
node must access. For this purpose, SCR creates two global communicators
during ``SCR_Init``. Both are defined in ``scr_globals.c``. The
``scr_comm_node`` communicator consists of all processes on the same
compute node. The ``scr_comm_node_across`` communicator consists of all
processes having the same rank within ``scr_comm_node``. Note that some
process has rank 0 in ``scr_comm_node`` for each node in the run. This
process is called the “node leader”.

To get the offset where each process should write its data, SCR first
sums up the sizes of all files on the node via a reduce on
``scr_comm_node``. The node leaders then execute a scan across nodes
using the ``scr_comm_node_across`` communicator to get a node offset. A
final scan within ``scr_comm_node`` produces the offset at which each
process should write its data.

TODO: discuss setting in flush descriptor stored in filemap under
dataset id and rank

TODO: discuss containers during a scavenge

TODO: should we copy redundancy data to containers as well?

Within a rank2file map file, the file-to-container map adds entries
under the ``SEG`` key for each file. An example entry looks like the
following:

::

     rank_2.ckpt
       SEG
         0
           FILE
             .scr/ctr.1.scr
           OFFSET
             224295
           LENGTH
             75705
         1
           FILE
             .scr/ctr.2.scr
           OFFSET
             0
           LENGTH
             300000
         2
           FILE
             .scr/ctr.3.scr
           OFFSET
             0
           LENGTH
             148591

The ``SEG`` key specifies file data as a list of numbered segments
starting from 0. Each segment specifies the length of file data, and the
name and offset at which it can be found within a container file.
Reading all segments in order produces the full sequence of bytes that
make up the file. The name of the container file is given as a relative
path from the dataset directory.

In the above example, the container size is set to 300000. This size is
smaller than normal to illustrate the various fields. The data for the
``rank_2.ckpt`` file is split among three segments. The first segment of
75705 bytes is in the container file named ``.scr/ctr.1.scr`` starting
at offset 224295. The next segment is 300000 bytes and is in
``.scr/ctr.2.scr`` starting at offset 0. The final segment of 148591
bytes are in ``.scr/ctr.3.scr`` starting at offset 0.
