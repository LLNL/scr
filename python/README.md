# SCR Python interface
Python programs can use SCR.

Given an SCR install at ``<scrdir>``,
the files in this directory are written to ``<scrdir>/share/scr/python``

## Implementation of the SCR Python interface
The scr module is implemented in ``scr.py``.

The ``scr.py`` module uses [CFFI](https://cffi.readthedocs.io) to load ``libscr.so``
and wrap a python interface around the SCR C functions.
During the SCR install process,
the absolute path to ``libscr.so`` is hardcoded in ``scr.py``.

For SCR developers,
the ``scr.py.in`` file must be maintained to track any changes to the SCR C API.

## Example using the SCR Python interface
The ``scrapp.py`` program demonstrates how one uses the scr module to checkpoint
and restart MPI processes within a python application.

This example can be executed as an MPI program, e.g.,
```
mpirun -np 2 python scrapp.py
```

Before running, one may need to set ``$LD_LIBRARY_PATH`` to point to
the libraries that ``libscr.so`` depends on.

It may also be necessary to add ``<scrdir>/share/scr/python`` to ``$PYTHONPATH``
so that the ``import scr`` statement can find ``scr.py``.
