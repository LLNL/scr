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
    class NewLauncher(JobLauncher):
      def launchruncmd():
        pass

      def killrun():
        pass

## Import the new class in `__init__.py`
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

And create an object of that class in :

    class AutoJobLauncher:
      def __new__(cls,joblauncher=None):
        ...
        if joblauncher == 'NewLauncher':
          return NewLauncher()
