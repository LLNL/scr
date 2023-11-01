#! /usr/bin/env python3

# Run this script after the final run in a job allocation
# to scavenge files from cache to parallel file system.

# add path holding scrjob to PYTHONPATH
import sys

sys.path.insert(0, '@X_LIBEXECDIR@/python')

import argparse

from scrjob.jobenv import JobEnv
from scrjob.postrun import postrun

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

    jobenv = JobEnv(prefix=args.prefix, launcher=args.joblauncher)

    postrun(jobenv=jobenv, verbose=args.verbose)
