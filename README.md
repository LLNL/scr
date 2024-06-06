# Scalable Checkpoint / Restart (SCR) Library

The Scalable Checkpoint / Restart (SCR) library enables MPI applications
to utilize distributed storage on Linux clusters to attain high file I/O
bandwidth for checkpointing, restarting, and output in large-scale jobs.
With SCR, jobs run more efficiently, recompute less work upon a failure,
and reduce load on critical shared resources such as the parallel file system.

## Users

Instructions to build and use SCR are hosted at [scr.readthedocs.io](https://scr.readthedocs.io/en/latest/).

For new users, the [Quick Start guide](https://scr.readthedocs.io/en/latest/users/quick.html)
shows one how to build and run an example using SCR.

For more detailed build instructions, refer to [Build SCR](https://scr.readthedocs.io/en/latest/users/build.html).

[![User Docs Status](https://readthedocs.org/projects/scr/badge/?version=latest)](https://scr.readthedocs.io/en/latest/?badge=latest)

## Contribute

As an open source project, we welcome contributions via pull requests,
as well as questions, feature requests, or bug reports via issues.
Please refer to both our [code of conduct](CODE_OF_CONDUCT.md) and our [contributing guidelines](CONTRIBUTING.md).

## Developers

Developer documentation is provided at [SCR-dev.ReadTheDocs.io](https://scr-dev.readthedocs.io/en/latest/).

[![Developer Docs Status](https://readthedocs.org/projects/scr-dev/badge/?version=latest)](https://scr-dev.readthedocs.io/en/latest/?badge=latest)

SCR uses components from [ECP-VeloC](https://github.com/ECP-VeloC),
which have their own [user](https://github.com/ECP-VeloC/component-user-docs)
and [developer](https://github.com/ECP-VeloC/component-dev-docs) docs.

A development build is useful for those who wish to modify how SCR works.
It checks out and builds SCR and many of its dependencies separately.
The process is more complicated than the user build described above,
but the development build is helpful when one intends to commit changes back to the project.

For a development build of SCR and its dependencies on SLURM systems,
one can use the bootstrap.sh script:

    git clone https://github.com/LLNL/scr.git
    cd scr

    ./bootstrap.sh

    cd build
    cmake -DCMAKE_INSTALL_PREFIX=../install ..
    make install

When using a debugger with SCR, one can build with the following flags to disable compiler optimizations:

    ./bootstrap.sh --debug

    cd build
    cmake -DCMAKE_INSTALL_PREFIX=../install -DCMAKE_BUILD_TYPE=Debug ..
    make install

One can then run a test program:

    cd examples
    srun -n4 -N4 ./test_api

For developers who may be installing SCR outside of an HPC cluster,
who are using Fedora, and who have sudo access,
the following steps install and activate most of the necessary base dependencies:

    sudo dnf groupinstall "Development Tools"
    sudo dnf install cmake gcc-c++ mpi mpi-devel environment-modules zlib-devel pdsh
    [restart shell]
    module load mpi

## Authors

Numerous people have [contributed](https://github.com/llnl/scr/graphs/contributors) to the SCR project.

To reference SCR in a publication, please cite the following paper:

* Adam Moody, Greg Bronevetsky, Kathryn Mohror, Bronis R. de Supinski, [Design, Modeling, and Evaluation of a Scalable Multi-level Checkpointing System](http://dl.acm.org/citation.cfm?id=1884666), LLNL-CONF-427742, Supercomputing 2010, New Orleans, LA, November 2010.

Additional information and research publications can be found here:

[https://computing.llnl.gov/projects/scalable-checkpoint-restart-for-mpi](https://computing.llnl.gov/projects/scalable-checkpoint-restart-for-mpi)
