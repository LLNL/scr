# Node health tests
SCR detects down nodes to support both its scalable restart and scavenge operatitons.
This directory contains tests to verify node health.
Each test is implemented as a separate class in this direcotry.

Different systems require a different set of tests.
For example, on a system with node-local SSDs,
one may want to test storage access to ensure the health of each SSD.
On a system with GPUs, one may want to execute test kernels on each GPU to exclude nodes with failed GPUs.
One may wish to add CPU or GPU performance tests to avoid nodes with slow hardware.
The set of checks that are actually executed depends on configuration as specified by the user.

There are three steps to add a new node test.

## Define a new node test class
One can add support for a new node test by extending
the `NodeTest` class and implementing the required interface.

See the `NodeTest` class in `nodetest.py`
for the interface definitions that one must implement, e.g.:

    >>: cat newtest.py
    from scrjob.nodetests import NodeTest

    class NewTest(NodeTest):
      def execute(self, nodes, jobenv):
        pass

## Import the new class in `__init__.py`
Add an import statement after the NodeTest and before the NodeTests imports.

Add a line to import the new class in the `__init__.py` file:

    from .newtest import NewTest

## Create a class object in `nodetests.py`
The `NodeTests` class instantiates and calls all active node tests.

Add a line to import the new class at the top of `nodetests.py`.

    from scrjob.nodetests import (
      ...
      NewTest
    )

Depending on the configuration, optionally instantiate a class object of the test,
and insert the test object in the list of `self.tests` in the `__init__` function of `NodeTests`:

    class NodeTests:
      def __init__(self):
        ...
        self.tests.append(NewTest())

## Add class file to `CMakeLists.txt`
Include the new node test in the list of files to be installed by CMake in `CMakeLists.txt`:

    SET(NODETESTS
      ...
      newtest.py
    )