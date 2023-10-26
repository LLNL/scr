#! /usr/bin/env python3

# add path holding scrjob to PYTHONPATH
import sys
sys.path.insert(0, '@X_LIBEXECDIR@/python')

import argparse

from scrjob.prerun import prerun
from scrjob.environment import SCR_Env

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

    scr_env = SCR_Env(prefix=args.prefix)

    prerun(scr_env=scr_env, verbose=args.verbose)
