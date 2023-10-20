#! /usr/bin/env python3

# this is a launcher script for list_down_nodes.py

import os, sys

if 'scrjob' not in sys.path:
    sys.path.insert(0, '/'.join(os.path.realpath(__file__).split('/')[:-2]))
    import scrjob

import argparse

from scrjob import scr_const
from scrjob.list_dir import list_dir
from scrjob.scr_environment import SCR_Env
from scrjob.scr_param import SCR_Param
from scrjob.resmgrs import AutoResourceManager
from scrjob.launchers import AutoJobLauncher
from scrjob.cli import SCRLog


# The main scr_list_down_nodes method.
# this method takes an scr_env, the contained resource manager will determine which methods above to use
def list_down_nodes(reason=False,
                    free=False,
                    nodes_down=[],
                    runtime_secs=None,
                    nodes=None,
                    scr_env=None,
                    log=None):

    if scr_env is None or scr_env.resmgr is None or scr_env.param is None:
        raise RuntimeError(
            'scr_list_down_nodes: ERROR: environment, resmgr, or param not set'
        )

    # check that we have a list of nodes before going any further
    if not nodes:
        nodes = scr_env.get_scr_nodelist()
    if not nodes:
        nodes = scr_env.resmgr.job_nodes()
    if not nodes:
        raise RuntimeError(
            'scr_list_down_nodes: ERROR: Nodeset must be specified or script must be run from within a job allocation.'
        )

    # drop any nodes that we are told are down
    for node in nodes_down:
        if node in nodes:
            nodes.remove(node)

    # get a dictionary of all unavailable (down or excluded) nodes and reason
    # keys are nodes and the values are the reasons
    unavailable = scr_env.resmgr.list_down_nodes_with_reason(nodes=nodes,
                                                             scr_env=scr_env)

    # TODO: read exclude list from a file, as well?

    # log each newly failed node, along with the reason
    if log:
        for node, reason in unavailable.items():
            note = node + ": " + reason
            log.event('NODE_FAIL', note=note, secs=runtime_secs)

    if not reason:
        return sorted(list(unavailable.keys()))

    return unavailable


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
