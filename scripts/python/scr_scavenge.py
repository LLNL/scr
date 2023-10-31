#! /usr/bin/env python3

# scavenge checkpoint files from cache to PFS

import os
import argparse

from scrjob.jobenv import JobEnv
from scrjob.scavenge import scavenge

if __name__ == '__main__':
    """This script is invoked to perform a scavenge operation.

    scr_scavenge.py is a wrapper to gather values and arrange parameters
    needed for a scavenge operation.

    When ready, the scavenge parameters are passed to the Joblauncher
    class to perform the scavenge operation.

    The output of the scavenge operation is both written to file and
    printed to screen.

    Exits with 1 on error, 0 on success.
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('-j',
                        '--jobset',
                        metavar='<nodeset>',
                        type=str,
                        default=None,
                        required=True,
                        help='Specify the nodeset.')
    parser.add_argument('-i',
                        '--id',
                        metavar='<id>',
                        type=str,
                        default=None,
                        required=True,
                        help='Specify the dataset id.')
    parser.add_argument('-f',
                        '--from',
                        metavar='<dir>',
                        type=str,
                        default=None,
                        required=True,
                        help='The control directory.')
    parser.add_argument('-t',
                        '--to',
                        metavar='<dir>',
                        type=str,
                        default=None,
                        required=True,
                        help='The prefix directory.')
    parser.add_argument('-u',
                        '--up',
                        metavar='<nodeset>',
                        type=str,
                        default=None,
                        help='Specify up nodes.')
    parser.add_argument('-d',
                        '--down',
                        metavar='<nodeset>',
                        type=str,
                        default=None,
                        help='Specify down nodes.')
    parser.add_argument('-v',
                        '--verbose',
                        action='store_true',
                        default=False,
                        help='Verbose output.')

    args = parser.parse_args()

    jobenv = JobEnv(prefix=None)

    nodes_job = jobenv.resmgr.expand_hosts(args.jobset)
    nodes_up = jobenv.resmgr.expand_hosts(args.up)
    nodes_down = jobenv.resmgr.expand_hosts(args.down)

    scavenge(nodes_job=nodes_job,
             nodes_up=nodes_up,
             nodes_down=nodes_down,
             dataset_id=args.id,
             cntldir=args['from'],
             prefixdir=args.to,
             verbose=args.verbose,
             jobenv=None)
