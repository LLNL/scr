.. _path:

File paths
==========

Overview
--------

The SCR library manipulates file and directory paths. To simplify this
task, it uses the ``scr_path`` data structure. There are a number of
functions to manipulate paths, including combining, slicing,
simplification, computing relative paths, and converting to/from
character strings.

This object stores file paths as a linked list of path components, where
a component is a character string separated by ‘/’ symbols. An empty
string is a valid component, and they are often found as the first
component in an absolute path, as in “/hello/world”, or as the last
component in a path ending with ‘/’. Components are indexed starting at
0.

Common functions
----------------

This section lists the most common functions used when dealing with
paths. For a full listing, refer to comments in ``scr_path.h``. The
implementation can be found in ``scr_path.c``.

Creating and freeing path objects
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

First, before using a path, one must allocate a path object.

::

     scr_path* path = scr_path_new();

This allocates an empty (or “null”) path having 0 components. One must
free the path when done with it.

::

     scr_path_delete(&path);

One may also create a path from a character string.

::

     scr_path* path = scr_path_from_str("/hello/world");

This splits the path into components at ‘/’ characters. In this example,
the resulting path would have three components, consisting of the empty
string, “hello”, and “world”. One can construct a path from a formatted
string.

::

     scr_path* path = scr_path_from_strf("/%s/%s/%d", dir1, dir2, id);

Or to make a full copy of a path as path2.

::

     scr_path* path2 = scr_path_dup(path);

Querying paths and converting them to character string
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

One can determine the number of components in a path.

::

     int components = scr_path_components(path);

A shortcut is available to identify a “null” path (i.e., a path with 0
components).

::

     int is_null_flag = scr_path_is_null(path);

This function returns 1 if the path has 0 components and 0 otherwise.
You can determine whether a path is absolute.

::

     int is_absolute_flag = scr_path_is_absolute(path);

This returns 1 if the path starts with an empty string and 0 otherwise.
The character representation of such a path starts with a ‘/’ character
or otherwise it is the empty string.

To get the number of characters in a path.

::

     size_t len = scr_path_strlen(path);

This count includes ‘/’ characters, but like the ``strlen`` function, it
excludes the terminating NULL character.

One can convert a path and return it as a newly allocated character
string.

::

     char* str = scr_path_strdup(path);
     scr_free(&str);

The caller is responsible for freeing the returned string.

Or one can copy the path into a buffer as a character string.

::

     char buf[bufsize];
     scr_path_strcpy(buf, bufsize, path);

Combining paths
~~~~~~~~~~~~~~~

There are functions to prepend and append entries to a path. To prepend
entries of path2 to path1 (does not affect path2).

::

     scr_path_prepend(path1, path2);

Similarly to append path2 to path1.

::

     scr_path_append(path1, path2);

Or one can insert entries of path2 into path1 at an arbitrary location.

::

     scr_path_insert(path1, offset, path2);

Here ``offset`` can be any value in the range :math:`[0, N]` where
:math:`N` is the number of components in ``path1``. With an offset of 0,
the entries of path2 are inserted before the first component of path1.
With an offset of :math:`N-1`, path2 in inserted before the last
component of path1. An offset of :math:`N` inserts path2 after the last
component of path1.

In addition, one may insert a string into a path using functions ending
with ``_str``, e.g., ``scr_path_prepend_str``. One may insert a
formatted string into a path using functions ending with ``_strf``,
e.g., ``scr_path_prepend_strf``.

Slicing paths
~~~~~~~~~~~~~

A number of functions are available to slice paths into smaller pieces.
First, one can chop components from the start and end of a path.

::

     scr_path_slice(path, offset, length);

This modifies ``path`` to keep length components starting from the
specified offset. The offset can be negative to count from the back. A
negative length means that all components are taken starting from the
offset to the end of the path.

A shortcut to chop off the last component.

::

     scr_path_dirname(path);

A shortcut that keeps only the last component.

::

     scr_path_basename(path);

The following function cuts a path in two at the specified offset. All
components starting at offset are returned as a newly allocated path.
The original path is modified to contain the beginning components.

::

     scr_path* path2 = scr_path_cut(path1, offset);

The above functions modify the source path. If one wants to take a piece
of a path without modifying the source, you can use the following
function. To create a new path which is a substring of a path.

::

     scr_path* path2 = scr_path_sub(path, offset, length);

The offset and length values have the same meaning as in
``scr_path_slice``.

Other path manipulation
~~~~~~~~~~~~~~~~~~~~~~~

A common need when dealing with paths is to simplify them to some
reduced form. The following function eliminates all “.”, “..”,
consecutive ‘/’, and trailing ‘/’ characters.

::

     scr_path_reduce(path);

As an example, the above function converts a path like
“/hello/world/../foo/bar/.././” to “/hello/foo”.

Since it is common to start from a string, reduce the path, and convert
back to a string, there is a shortcut that allocates a new, reduced path
as a string.

::

     char* reduced_str = scr_path_strdup_reduce_str(str);
     scr_free(&reduced_str);

The caller is responsible for freeing the returned string.

Another useful function is to compute one path relative to another.

::

     scr_path* path = scr_path_relative(src, dst);

This function computes ``dst`` as a path relative to ``src`` and returns
the result as a newly allocated path object. For example, if ``src`` is
“/hello/world” and ``dst`` is “/hello/foo”, the returned path would be
“../foo”.
