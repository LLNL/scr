#! /usr/bin/env python3

# Determine whether the job script should exit or launch another run.
# Checks that:
#   - a halt condition has not been set
#   - there is sufficient time remaining in the allocation
#   - there are sufficient healthy nodes in the allocation
# Exits with 0 if job should exit, 1 otherwise

# add path holding scrjob to PYTHONPATH
import sys
sys.path.insert(0, '@X_LIBEXECDIR@/python')

import argparse

from scrjob.environment import JobEnv
from scrjob.should_exit import should_exit


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p',
                        '--prefix',
                        metavar='<dir>',
                        type=str,
                        default=None,
                        help='Specify the prefix directory.')
    parser.add_argument('--down',
                        metavar='<nodelist>',
                        type=str,
                        default=None,
                        help='Specify list of down nodes.')
    parser.add_argument('-v',
                        '--verbose',
                        action='store_true',
                        default=False,
                        help='Verbose output.')

    args = parser.parse_args()

    jobenv = JobEnv(prefix=args.prefix)

    down_nodes = []
    if args.down:
        down_nodes = jobenv.resmgr.expand_hosts(args.down)

    if not should_exit(jobenv, keep_down=down_nodes, verbose=args.verbose):
        sys.exit(1)
