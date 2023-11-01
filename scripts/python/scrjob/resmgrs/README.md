# Resource Managers
SCR interacts with the resource manager for various tasks,
like acquiring the allocation id, the list of compute nodes,
and the expected end time of the allocation.

Base class:
- ``resource manager.py`` - Defines the ``ResourceManager`` base class that each resource manager implements
- ``auto.py``- Defines the ``AutoResourceManager`` class that instantiates a resource manager depending on the system environment

Existing resource manager classes:
- ``flux.py``- Flux
- ``lsf.py``- IBM LSF
- ``pbsalps.py``- Cray Torque PBS with ALPS launcher
- ``pmix.py``- PMIX (not functional)
- ``slurm.py``- SLURM

# Adding a new resource manager

The steps to add a new resource manager are described below.

## Define a new resource manager class
One can add support for a new resource manager by extending
the `ResourceManager` class and implementing the required interface.
See the `ResourceManager` class in `resourcemanager.py`
for the interface definitions that one must implement, e.g.:

    >>: cat newrm.py
    from scrjob.resmgrs import ResourceManager

    class NewRM(ResourceManager):
      def job_id():
        pass

      def end_time():
        pass

      def job_nodes():
        pass

      def down_nodes():
        pass

## Import the new class in `__init__.py`
Add a line to import the new class in the `__init__.py` file
after the ``ResourceManager`` and before the ``AutoResourceManager`` imports

    from .resourcemanager import ResourceManager
    ...
    from .newrm import NewRM
    ...
    from .auto import AutoResourceManager

## Create a class object in `auto.py`
Users often create a new resource manager object through the `AutoResourceManager` function.

The ``ResourceManager`` type, when not provided on instantiation, is determined by a constant in ``scrjob/config.py``:

    rm = new AutoResourceManager(resmgr='NewRM')

Add lines to import the new class and create an object to `auto.py`.

    from scrjob.resmgrs import (
      ...
      NewRM
    )

And create an object of the new class in the `__new__` method of `AutoResourceManager`:

    class AutoResourceManager:
      def __new__(cls,resmgr=None):
        ...
        if resmgr == 'NewRM':
          return NewRM()

## Add class file to `CMakeLists.txt`
Include the new resource manager in the list of files to be installed by CMake in `CMakeLists.txt`:

    SET(RESOURCEMANAGERS
      ...
      newrm.py
    )
