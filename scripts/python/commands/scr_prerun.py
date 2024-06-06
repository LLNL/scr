#! /usr/bin/env python3

# add path holding scrjob to PYTHONPATH
import sys

sys.path.insert(0, '@X_LIBEXECDIR@/python')

import argparse

from scrjob.prerun import prerun
from scrjob.jobenv import JobEnv

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
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

    jobenv = JobEnv(prefix=args.prefix)

    prerun(jobenv=jobenv, verbose=args.verbose)
