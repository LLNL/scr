#! /usr/bin/env python3

# Run this script after the final run in a job allocation
# to scavenge files from cache to parallel file system.

import argparse

from scrjob.postrun import postrun
from scrjob.scr_environment import SCR_Env
from scrjob.scr_param import SCR_Param
from scrjob.resmgrs import AutoResourceManager
from scrjob.launchers import AutoJobLauncher

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

    postrun(prefix_dir=args.prefix, scr_env=scr_env, verbose=args.verbose)
