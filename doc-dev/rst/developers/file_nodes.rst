.. _nodes_file:

Nodes file
----------

The nodes file is kept in the prefix directory, and it tracks how many
nodes were used by the previous run. Internally, the data of the nodes
file is organized as a hash. Here are the contents of an example nodes
file.

::

     NODES
       4

In this example, the previous run which ran on this node used 4 nodes.
The number of nodes is computed by finding the maximum size of
``scr_comm_level`` across all tasks in the MPI job. The master process
on each node writes the nodes file to the control directory.

Before restarting a run, SCR uses information in this file to determine
whether there are sufficient healthy nodes remaining in the allocation
to run the job. If this file does not exist, SCR assumes the job needs
every node in the allocation. Otherwise, it assumes the next run will
use the same number of nodes as the previous run, which is recorded in
this file.
