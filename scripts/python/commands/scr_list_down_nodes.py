#! /usr/bin/env python3

# this is a launcher script for list_down_nodes.py

# add path holding scrjob to PYTHONPATH
import sys

sys.path.insert(0, '@X_LIBEXECDIR@/python')

import argparse

from scrjob import hostlist
from scrjob.jobenv import JobEnv
from scrjob.cli import SCRLog
from scrjob.list_down_nodes import list_down_nodes

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-j',
                        '--joblauncher',
                        type=str,
                        required=True,
                        help='Specify the job launcher.')
    parser.add_argument('-r',
                        '--reason',
                        action='store_true',
                        default=False,
                        help='Print reason node is down.')
    parser.add_argument(
        '-f',
        '--free',
        action='store_true',
        default=False,
        help=
        'Test required drive space based on free amount, rather than capacity.'
    )
    parser.add_argument('-d',
                        '--down',
                        metavar='<nodeset>',
                        type=str,
                        default=None,
                        help='Consider nodes to be down without testing.')
    parser.add_argument('-l',
                        '--log',
                        action='store_true',
                        default=False,
                        help='Add entry to SCR log for each down node.')
    parser.add_argument(
        '-s',
        '--secs',
        metavar='N',
        type=str,
        default=None,
        help='Specify number of seconds job has been running for SCR log.')
    parser.add_argument('nodeset',
                        nargs='*',
                        default=None,
                        help='Specify the set of nodes to check.')

    args = parser.parse_args()

    jobenv = JobEnv(launcher=args.joblauncher)

    # create log object if asked to log down nodes
    log = None
    if args.log:
        prefix = jobenv.dir_prefix()
        jobid = jobenv.resmgr.job_id()
        user = jobenv.user()
        log = SCRLog(prefix, jobid, user=user)

    node_list = hostlist.expand_hosts(args.nodeset)
    down_list = hostlist.expand_hosts(args.down)

    down = list_down_nodes(jobenv,
                           nodes=node_list,
                           nodes_down=down_list,
                           free=args.free,
                           reason=args.reason,
                           log=log,
                           secs=args.secs)

    if args.reason:
        # list each node and the reason each is down
        for node in sorted(down.keys()):
            print(node + ': ' + down[node])
    else:
        # simply print the list of down nodes
        downstr = hostlist.compress_hosts(down)
        print(downstr)
