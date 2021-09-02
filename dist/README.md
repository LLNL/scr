# SCR release tarball
The builddist script creates an SCR release tarball.

```bash
    ./builddist develop
```

This tarball is added as a binary attachment to the corresponding SCR release page.
This contains source for SCR, its ECP dependencies, LWGRP, and DTCMP.
It also contains a set of top-level CMake files that compiles all source files into a single libscr library.

# Steps to add a new release
To add a new release:
1. Define new CMake files if needed (TODO: support multiple versions of top-level CMake files)
2. Edit builddist to define the appropriate tags for all packages
3. Run builddist for appropriate tag
4. Test build and run with resulting tarball
5. Attach tarball to github release page (attach binary)
6. Update SCR readthedocs to describe builds from this tarball
