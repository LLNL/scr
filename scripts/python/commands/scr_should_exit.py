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

from scrjob import hostlist
from scrjob.jobenv import JobEnv
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
    parser.add_argument('--min-nodes',
                        metavar='<N>',
                        type=int,
                        default=None,
                        help='Required number of nodes to run.')
    parser.add_argument('--runs',
                        metavar='<N>',
                        type=int,
                        default=None,
                        help='Number of runs remaining.')
    parser.add_argument('-v',
                        '--verbose',
                        action='store_true',
                        default=False,
                        help='Verbose output.')

    args = parser.parse_args()

    jobenv = JobEnv(prefix=args.prefix)

    down_nodes = []
    if args.down:
        down_nodes = hostlist.expand_hosts(args.down)

    if not should_exit(jobenv,
                       down_nodes=down_nodes,
                       min_nodes=args.min_nodes,
                       verbose=args.verbose):
        # No need to exit detected yet.

        # TODO: change the SCR library to update a run counter in the halt file
        # Job should exit if given --runs=0.
        # This seems a bit trivial, but it does simplify the user's job script.
        # We check this last, since it's better to report other halt conditions if set.
        if args.runs == 0:
            if args.verbose:
                print(f'runs_remaining=0')
            sys.exit(0)

        # use exit code 1 to indicate that job does not need to stop
        sys.exit(1)
