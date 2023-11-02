# Runs on each compute node to check health of each node.
# For example, this may verify that local storage is accessible
# and has sufficient capactiy.
#
# The script prints PASS if all checks pass, and FAIL if any test fails.
# The output is parsed by another script to verify that a node
# is good and otherwise to report the reason the node failed.
#
# This currently checks:
#   control directory
#     is available,
#     has the proper size,
#     and is writable
#   cache directory
#     is available,
#     has the proper size,
#     and is writable
#
# TODO: generalize to support a configurable set of tests
# To support modularity and user-defined tests, each test
# should be implemented in its own python script.
# This script should instantiate and execute each test as
# specified by the user.
#
# Other tests that could be useful:
#   run GPU tests to verify functionality
#   run GPU/CPU performance tests to verify performance

# add path holding scrjob to PYTHONPATH
import os
import sys

sys.path.insert(0, '/'.join(os.path.realpath(__file__).split('/')[:-2]))

import argparse


def check_size(path, req_bytes, free):
    """Verify path has specified bytes of total capacity or free space."""

    # stat the path
    try:
        statvfs = os.statvfs(path)
    except Exception as e:
        raise RuntimeError('Could not access directory: ' + path + str(e))

    # if --free was given, check free space on drive
    # otherwise get total drive capacity
    have_bytes = 0
    if free:
        # free bytes
        have_bytes = statvfs.f_bsize * statvfs.f_bavail
    else:
        # max capacity
        have_bytes = statvfs.f_bsize * statvfs.f_blocks

    # compare to required number of bytes
    if have_bytes < req_bytes:
        raise RuntimeError('Insufficient space in directory: ' + path +
                           ', expected ' + str(req_bytes) + ', found ' +
                           str(have_bytes))


def check_writable(path):
    """Attempt to write and delete a small file to given path."""

    # check that we can access the directory
    # (perl code ran an ls)
    # the docs suggest not to ask if access available, but to just try to access:
    # https://docs.python.org/3/library/os.html?highlight=os%20access#os.access

    testfile = os.path.join(path, 'testfile.txt')
    try:
        os.makedirs(path, exist_ok=True)
        with open(testfile, 'w') as f:
            f.write('test')
    except PermissionError:
        raise RuntimeError('Lack permission to write test file: ' + testfile)
    except Exception as e:
        raise RuntimeError('Could not touch test file: ' + testfile + str(e))

    try:
        os.remove(testfile)
    except PermissionError:
        raise RuntimeError('Lack permission to rm test file: ' + testfile)
    except FileNotFoundError:
        pass
    except Exception as e:
        raise RuntimeError('Could not rm test file: ' + testfile + str(e))


def scr_check_node(free=False, cntl_list=None, cache_list=None):
    # The control directory is where SCR stores data about the state of the run
    # This is typically small and stored in fast storage like /dev/shm.
    #
    # The cache directory is where SCR stores dataset and redundancy data.

    checkdict = {}
    types = ['cntl', 'cache']
    for atype in types:
        if ((atype == 'cntl' and cntl_list is None)
                or (atype == 'cache' and cache_list is None)):
            types.remove(atype)
            continue

        # multiple paths can be specified, separated by commas
        if atype == 'cntl':
            dirs = cntl_list.split(',')
        else:
            dirs = cache_list.split(',')

        checkdict[atype] = {}
        for adir in dirs:
            # drop any trailing slash from the path
            if adir.endswith('/'):
                adir = adir[:-1]

            # TODO: support parsing of units, like 100GB
            # path may optionally be followed by a required size
            # like /ssd:100000000000
            if ':' in adir:
                parts = adir.split(':')
                checkdict[atype][parts[0]] = {}
                checkdict[atype][parts[0]]['bytes'] = int(parts[1])
            else:
                checkdict[atype][adir] = {}
                checkdict[atype][adir]['bytes'] = None

    # check that we can access the directory
    for atype in checkdict:
        # if a size is defined, check that the total size is enough
        dirs = list(checkdict[atype])
        for adir in dirs:
            req_bytes = checkdict[atype][adir]['bytes']
            if req_bytes is not None:
                check_size(adir, req_bytes, free)

        # attempt to write to directory
        for adir in dirs:
            check_writable(adir)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Checks that the current node is healthy')
    parser.add_argument(
        '--free',
        action='store_true',
        default=False,
        help=
        'Check that free capacity of drive meets limit, checks total capacity of drive otherwise.'
    )
    parser.add_argument('--cntl',
                        metavar='<dir>',
                        type=str,
                        default=None,
                        help='Specify the SCR control directory.')
    parser.add_argument('--cache',
                        metavar='<dir>',
                        type=str,
                        default=None,
                        help='Specify the SCR cache directory.')

    args = parser.parse_args()

    try:
        scr_check_node(free=args.free,
                       cntl_list=args.cntl,
                       cache_list=args.cache)
        print('scr_check_node: PASS')
    except Exception as e:
        print('scr_check_node: FAIL: ' + str(e))
        sys.exit(1)
