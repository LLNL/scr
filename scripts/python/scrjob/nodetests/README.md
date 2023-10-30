# Node health checks
SCR detects down nodes to support both its scalable restart and scavenge operatitons.
This directory contains tests to verify node health.

Different systems require a different set of tests.
For example, on a system with node-local SSDs,
one may want to test storage access to ensure the health of each SSD.
On a system with GPUs, one may want to execute test kernels on each GPU to exclude nodes with failed GPUs.
One may wish to add CPU or GPU performance tests to avoid nodes with slow hardware.

Currently, all tests are implemented in the ``Nodetests`` class.
Methods on this class are invoked by the base ``ResourceManager`` class when detecting down nodes.
The set of tests that are executed is defined by the ``ResourceManager`` subclass.
For example, ``resmgr/slurm.py`` invokes tests to ``ping`` each compute node and test access and capacity of node local storage.

## TODO
The plan is to support a wide set of tests,
where each test is implemented as a separate class in this direcotry.

In addition to pre-existing tests installed with SCR,
this design will enable users to easily add new tests specific to their environment.

The set of checks that are executed will depend on a configuration file given by the user.
