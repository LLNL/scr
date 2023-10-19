# Job Launchers
SCR interacts with the MPI job launch commands in various ways
like constructing the command to launch a run while excluding
known down nodes and killing a hanging run.

There are three steps to add a new job launcher.

## Define a new job launcher class
One can add support for a new job launcher by extending
the `JobLauncher` class and implementing the required interface.

See the `JobLauncher` class in `joblauncher.py`
for the interface definitions that one must implement, e.g.:

    >>: cat newlauncher.py
    from pyfe.joblauncher import JobLauncher

    class NewLauncher(JobLauncher):
      def launch_run_cmd():
        pass

      def killrun():
        pass

## Import the new class in `__init__.py`
Add the new import after the JobLauncher and before the AutoJobLauncher imports

Add a line to import the new class in the `__init__.py` file:

    from .newlauncher import NewLauncher

## Create a class object in `auto.py`
Users often create new job launcher objects through the `AutoJobLauncher` function.

    launcher = new AutoJobLauncher(joblauncher='NewLauncher')

Add lines to import the new class and create an object to `auto.py`.

    from scrjob.launchers import (
      ...
      NewLauncher
    )

And create an object of that class in the `__new__` method of `AutoJobLauncher`:

    class AutoJobLauncher:
      def __new__(cls,joblauncher=None):
        ...
        if joblauncher == 'NewLauncher':
          return NewLauncher()

Usage for your new job launcher will be: `scr_run.py NewLauncher <launcher args> <cmd> <cmd args>`

You may also provide a named script to shorten the usage:

    cp pyfe/scr_srun.py pyfe/scr_newlauncher.py
    sed -i 's/srun/NewLauncher/g' scr_newlauncher.py

This will allow your launcher to be ran as: `scr_newlauncher.py <launcher args> <cmd> <cmd args>`

_If a new named script is created ensure to add it to pyfe/CMakeLists.txt as well_

## Add class file to `CMakeLists.txt`
Include the new job launcher in the list of files to be installed by CMake in `CMakeLists.txt`:

    SET(JOBLAUNCHERS
      ...
      newlauncher.py
    )
