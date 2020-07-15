
GitLab CI Testing 
=================

SCR's GitLab testing suite does a few things:

1. Test that SCR will build with CMake
2. Trigger SCR's built-in testing with `make test` (uses ctest)
3. Test that SCR will build with Spack
4. Trigger more advanced test by running the `testing/TEST` script
