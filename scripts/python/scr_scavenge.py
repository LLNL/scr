# scavenge checkpoint files from cache to PFS

import os
import argparse

from scrjob import hostlist
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
    parser.add_argument('-p',
                        '--prefix',
                        metavar='<dir>',
                        type=str,
                        default=None,
                        required=True,
                        help='The prefix directory.')
    parser.add_argument('-v',
                        '--verbose',
                        action='store_true',
                        default=False,
                        help='Verbose output.')

    args = parser.parse_args()

    jobenv = JobEnv(prefix=args.prefix)

    nodes = hostlist.expand_hosts(args.nodes) if args.nodes else []

    scavenge(jobenv,
             nodes,
             dataset_id=args.id,
             cntldir=args['from'],
             verbose=args.verbose)
