#! /usr/bin/env python3

import sys
import argparse

from scrjob.environment import SCR_Env
from scrjob.list_dir import list_dir

if __name__ == '__main__':
    """This is an external driver for the internal list_dir method.

    This script is for stand-alone purposes and is not used within other
    scripts

    This script can be used to test the list_dir method of list_dir.py
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-u',
                        '--user',
                        default=None,
                        metavar='<user>',
                        type=str,
                        help='Specify username.')
    parser.add_argument('-j',
                        '--jobid',
                        default=None,
                        metavar='<id>',
                        type=str,
                        help='Specify jobid.')
    parser.add_argument('-b',
                        '--base',
                        action='store_true',
                        default=False,
                        help='List base portion of cache/control directory')
    parser.add_argument('-p',
                        '--prefix',
                        default=None,
                        metavar='<id>',
                        type=str,
                        help='Specify the prefix directory.')
    parser.add_argument('control/cache',
                        choices=['control', 'cache'],
                        metavar='<control | cache>',
                        nargs='?',
                        default=None,
                        help='Specify the directory to list.')

    args = parser.parse_args()

    if args['control/cache'] is None:
        print('Control or cache must be specified.')
        sys.exit(1)

    # ensure scr_env is set
    scr_env = SCR_Env(prefix=args['prefix'])

    dirs = list_dir(user=args['user'],
                    jobid=args['jobid'],
                    base=args['base'],
                    runcmd=args['control/cache'],
                    scr_env=scr_env)
    print(' '.join(dirs))
