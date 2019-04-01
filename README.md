# Scalable Checkpoint / Restart (SCR) Library

The Scalable Checkpoint / Restart (SCR) library enables MPI applications
to utilize distributed storage on Linux clusters to attain high file I/O
bandwidth for checkpointing and restarting large-scale jobs. With SCR,
jobs run more efficiently, recompute less work upon a failure, and reduce
load on critical shared resources such as the parallel file system.

Detailed usage is provided at [SCR.ReadTheDocs.io](http://scr.readthedocs.io/en/latest/).

[![User Docs Status](https://readthedocs.org/projects/scr/badge/?version=latest)](https://scr.readthedocs.io/en/latest/?badge=latest)

## Quickstart

SCR uses the CMake build system and we recommend out-of-source builds.

```shell
git clone git@github.com:llnl/scr.git
mkdir build
mkdir install

cd build
cmake -DCMAKE_INSTALL_PREFIX=../install ../scr
make
make install
make test
```

Some useful CMake command line options:

- `-DCMAKE_INSTALL_PREFIX=[path]`: Place to install the SCR library
- `-DCMAKE_BUILD_TYPE=[Debug/Release]`: Build with debugging or optimizations
- `-DBUILD_PDSH=[OFF/ON]`: CMake can automatically download and build the PDSH dependency
- `-DWITH_PDSH_PREFIX=[path to PDSH]`: Path to an existing PDSH installation (should not be used with `BUILD_PDSH`)
- `-DWITH_DTCMP_PREFIX=[path to DTCMP]`
- `-DWITH_YOGRT_PREFIX=[path to YOGRT]`
- `-DSCR_ASYNC_API=[CRAY_DW/INTEL_CPPR/IBM_BBAPI/NONE]`
- `-DSCR_RESOURCE_MANAGER=[SLURM/APRUN/PMIX/LSF/NONE]`

### Dependencies

- C (with support for C++ and Fortran)
- MPI
- CMake, Version 2.8+
- [PDSH](https://github.com/grondo/pdsh)
- [DTCMP](https://github.com/llnl/dtcmp) (optional)
- [libYOGRT](https://github.com/llnl/libyogrt) (optional)
- MySQL (optional)

## Configuration Files

SCR searches the following locations in the following order for a parameter value, taking the first value it finds.

1. Environment variables,
2. User configuration file,
3. System configuration file,
4. Compile-time constants.

To find a user configuration file, SCR looks for a file named `.scrconf` in the prefix directory (note the leading dot).
Alternatively, one may specify the name and location of the user configuration file by setting the `SCR_CONF_FILE` environment variable at run time.
This repository includes some example configuration files (`scr.conf.template`, `scr.user.conf.template`, and `examples/test.conf`).

## Authors

Numerous people have [contributed](https://github.com/llnl/scr/graphs/contributors) to the SCR project.

To reference SCR in a publication, please cite the following paper:

* Adam Moody, Greg Bronevetsky, Kathryn Mohror, Bronis R. de Supinski, [Design, Modeling, and Evaluation of a Scalable Multi-level Checkpointing System](http://dl.acm.org/citation.cfm?id=1884666), LLNL-CONF-427742, Supercomputing 2010, New Orleans, LA, November 2010.

Additional information and research publications can be found here:

[http://computation.llnl.gov/projects/scalable-checkpoint-restart-for-mpi](http://computation.llnl.gov/projects/scalable-checkpoint-restart-for-mpi)

## Developers

Developer documentation is provided at [SCR-dev.ReadTheDocs.io](http://scr-dev.readthedocs.io/en/latest/).

[![Developer Docs Status](https://readthedocs.org/projects/scr-dev/badge/?version=latest)](https://scr-dev.readthedocs.io/en/latest/?badge=latest)
