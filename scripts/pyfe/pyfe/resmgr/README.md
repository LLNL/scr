# Resource Managers
SCR interacts with the resource manager for various tasks,
like acquiring the allocation id, the list of compute nodes,
and the expected end time of the allocation.

There are three steps to add a new resource manager.

## Define a new resource manager class
One can add support for a new resource manager by extending
the `ResourceManager` class and implementing the required interface.

See the `ResourceManager` class in `resourcemanager.py`
for the interface definitions that one must implement, e.g.:

    >>: cat newrm.py
    from pyfe.resmgr import ResourceManager

    class NewRM(ResourceManager):
      def get_job_id():
        pass

      def get_scr_end_time():
        pass

      def get_jobnodes():
        pass

      def get_downnodes():
        pass

## Import the new class in `__init__.py`
Add the new import after the ResourceManager and before the AutoResourceManager imports
Add a line to import the new class in the `__init__.py` file:

    from .newrm import NewRM

## Create a class object in `auto.py`
Users often create new resource manager objects through the `AutoResourceManager` function.
The ResourceManager type, when not provided on instantiation, is determined by a constant in scr\_const.py

    rm = new AutoResourceManager(resmgr='NewRM')

Add lines to import the new class and create an object to `auto.py`.

    from pyfe.resmgr import (
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
