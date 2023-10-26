#! /usr/bin/env python3

# Determine whether the job script should exit or launch another run.
# Checks that:
#   - a halt condition has not been set
#   - there is sufficient time remaining in the allocation
#   - there are sufficient healthy nodes in the allocation
# Exits with 0 if job should exit, 1 otherwise

import os
import sys
import argparse

from scrjob.environment import SCR_Env
from scrjob.scr_param import SCR_Param
from scrjob.resmgrs import AutoResourceManager
from scrjob.launchers import AutoJobLauncher
from scrjob.should_exit import should_exit


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-j',
                        '--joblauncher',
                        type=str,
                        required=True,
                        help='Specify the job launcher.')
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
                        help='Specify list of nodes to consider to be down.')
    parser.add_argument('--check-capacity',
                        action='store_true',
                        help='Whether to check drive capacity instead of free space.')
    parser.add_argument('-v',
                        '--verbose',
                        action='store_true',
                        default=False,
                        help='Verbose output.')

    args = parser.parse_args()

    scr_env = SCR_Env(prefix=args.prefix)
    scr_env.param = SCR_Param()
    scr_env.resmgr = AutoResourceManager()
    scr_env.launcher = AutoJobLauncher(args.joblauncher)

    down_nodes = []
    if args.down:
        down_nodes = scr_env.resmgr.expand_hosts(args.down)

    first_run = args.check_capacity

    if not should_exit(scr_env, keep_down=down_nodes, first_run=first_run, verbose=args.verbose):
        sys.exit(1)
