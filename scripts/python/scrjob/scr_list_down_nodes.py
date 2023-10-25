#! /usr/bin/env python3

# this is a launcher script for list_down_nodes.py

import os, sys

if 'scrjob' not in sys.path:
    sys.path.insert(0, '/'.join(os.path.realpath(__file__).split('/')[:-2]))
    import scrjob

import argparse

from scrjob import scr_const
from scrjob.list_down_nodes import list_down_nodes
from scrjob.scr_environment import SCR_Env
from scrjob.scr_param import SCR_Param
from scrjob.resmgrs import AutoResourceManager
from scrjob.launchers import AutoJobLauncher
from scrjob.cli import SCRLog


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

    scr_env = SCR_Env()
    scr_env.resmgr = AutoResourceManager()
    scr_env.param = SCR_Param()
    scr_env.launcher = AutoJobLauncher(args.joblauncher)

    # create log object if asked to log down nodes
    log = None
    if args.log:
        prefix = scr_env.get_prefix()
        jobid = scr_env.resmgr.job_id()
        user = scr_env.get_user()
        log = SCRLog(prefix, jobid, user=user)

    node_list = scr_env.resmgr.expand_hosts(args.nodeset)
    down_list = scr_env.resmgr.expand_hosts(args.down)

    down = list_down_nodes(reason=args.reason,
                           free=args.free,
                           nodes_down=down_list,
                           runtime_secs=args.secs,
                           nodes=node_list,
                           scr_env=scr_env,
                           log=log)

    if args.reason:
        # list each node and the reason each is down
        for node in sorted(down.keys()):
            print(node + ': ' + down[node])
    else:
        # simply print the list of down node in range syntax
        # cast unavailable to a list to get only the keys of the dictionary
        down_range = scr_env.resmgr.compress_hosts(down)
        print(down_range)
