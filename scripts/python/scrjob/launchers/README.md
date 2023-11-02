# Job Launchers
SCR interacts with the MPI job launch commands in various ways
like constructing the command to launch a run while excluding
known down nodes and killing a hanging run.

Base class: 
- ``joblauncher.py`` - Defines the ``JobLauncher`` base class that each job launcher implements
- ``auto.py`` - Defines the ``AutoJobLauncher`` class that instantiates a job launcher depending on the system environment

Existing job launcher classes:
- ``aprun.py`` - Cray ALPS ``aprun``
- ``flux.py`` - Flux ``flux run``
- ``jsrun.py`` - IBM LSF ``jsrun``
- ``lrun.py`` - LLNL ``lrun``
- ``mpirun.py`` - generic ``mpirun`` (not functional)
- ``srun.py`` - SLURM ``srun``

# Adding a new job launcher

The steps to add a new job launcher are described below.

## Define a new job launcher class
One can add support for a new job launcher by extending
the `JobLauncher` class.
See the `JobLauncher` class in `joblauncher.py`
for the interface definitions that one must implement, e.g.:

    >>: cat newlauncher.py
    from scrjob.launchers import JobLauncher

    class NewLauncher(JobLauncher):
      def launch_run():
        pass

      def wait_run():
        pass

      def kill_run():
        pass

## Import the new class in `__init__.py`
Add a line to import the new class in the `__init__.py` file
after the ``JobLauncher`` and before the ``AutoJobLauncher`` imports.

    from .joblauncher import JobLauncher
    ...
    from .newlauncher import NewLauncher
    ...
    from .auto import AutoJobLauncher

## Create a class object in `auto.py`
Users often create a new job launcher object through the `AutoJobLauncher` function.

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

## Add class file to `CMakeLists.txt`
Include the new job launcher in the list of files to be installed by CMake in `CMakeLists.txt`:

    SET(JOBLAUNCHERS
      ...
      newlauncher.py
    )
