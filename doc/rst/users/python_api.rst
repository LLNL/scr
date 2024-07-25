.. highlight:: python

.. _sec-lib-python-api:

SCR Python API
==============

SCR Python APIs allow user to access SCR functionality through an Python interface. 
The Python APIs wrap the SCR C functions by using [CFFI](https://cffi.readthedocs.io). 
A detailed description of the SCR's internal API are descripted in :doc:`api`. 


SCR Python module
-----------------

Python programs can call SCR library functions through the ``scr.py`` module.

Implementation of the SCR Python module
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``scr.py`` module uses [CFFI](https://cffi.readthedocs.io) to load ``libscr.so``
and wrap a Python interface around the SCR C functions.
During the SCR install process,
the absolute path to ``libscr.so`` is hardcoded in ``scr.py``.

Installing the SCR Python module
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Given an SCR install at ``<scrdir>``,
files from this directory are written to ``<scrdir>/share/scr/python``.

After installing SCR,
one can use ``setup.py`` to install ``scr.py`` into a Python environment:

.. code-block:: python

    cd <scrdir>/share/scr/python
    python setup.py install


Alternatively, one can add ``<scrdir>/share/scr/python`` to ``$PYTHONPATH``
so that an ``import scr`` statement finds ``scr.py``.

Using the SCR Python module
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For usage, refer to the module docstrings:

.. code-block:: python
    python -c 'import scr; help(scr)'
    

The scr module expects an active MPI environment, e.g.:

.. code-block:: python
    from mpi4py import MPI
    import scr
    scr.init()


The ``scr_example.py`` program demonstrates how one uses the scr module
to checkpoint and restart MPI processes within a Python application.

This example is an MPI program, e.g.:


.. code-block:: python
    mpirun -np 2 python scr_example.py


Depending on how the SCR library was built,
one may also need to set ``$LD_LIBRARY_PATH`` to point to
libraries that ``libscr.so`` depends on before running.