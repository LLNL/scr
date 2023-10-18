#! /usr/bin/env python3

# scr_env.py
# this is a standalone script which queries the class SCR_Env
# SCR_Env contains general values from the environment

import os, sys

if 'scrjob' not in sys.path:
    sys.path.insert(0, '/'.join(os.path.realpath(__file__).split('/')[:-2]))
    import scrjob

import argparse
from scrjob.scr_environment import SCR_Env
from scrjob.resmgrs import AutoResourceManager
from scrjob.scr_param import SCR_Param
from scrjob.launchers import AutoJobLauncher


def printobject(obj, objname):
    for attr in dir(obj):
        if attr.startswith('__'):
            continue
        thing = getattr(obj, attr)
        if thing is not None and (attr == 'resmgr' or attr == 'launcher'
                                  or attr == 'param'):
            printobject(thing, objname + '.' + attr)
        elif type(thing) is dict:
            print(objname + '.' + attr + ' = {}')
            for key in thing:
                print(objname + '.' + attr + '[' + key + '] = ' +
                      str(thing[key]))
        else:
            print(objname + '.' + attr + ' = ' + str(thing))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(add_help=False,
                                     argument_default=argparse.SUPPRESS,
                                     prog='scr_env')
    parser.add_argument('-h',
                        '--help',
                        action='store_true',
                        help='Show this help message and exit.')
    parser.add_argument('-u',
                        '--user',
                        action='store_true',
                        help='List the username of current job.')
    parser.add_argument('-j',
                        '--jobid',
                        action='store_true',
                        help='List the job id of the current job.')
    parser.add_argument('-e',
                        '--endtime',
                        action='store_true',
                        help='List the end time of the current job.')
    parser.add_argument('-n',
                        '--nodes',
                        action='store_true',
                        help='List the nodeset the current job is using.')
    parser.add_argument(
        '-d',
        '--down',
        action='store_true',
        help=
        'List any nodes of the job\'s nodeset that the resource manager knows to be down.'
    )
    parser.add_argument('-p',
                        '--prefix',
                        metavar='<dir>',
                        type=str,
                        default=None,
                        help='Specify the prefix directory.')
    parser.add_argument('-r',
                        '--runnodes',
                        action='store_true',
                        help='List the number of nodes used in the last run.')
    args = vars(parser.parse_args())

    scr_env = SCR_Env(prefix=args['prefix'])
    scr_env.resmgr = AutoResourceManager()
    scr_env.launcher = AutoJobLauncher()
    scr_env.param = SCR_Param()

    if len(args) == 0:
        printobject(scr_env, 'scr_env')
    elif 'help' in args:
        parser.print_help()
    else:
        if 'user' in args:
            print(str(scr_env.get_user()), end='')
        if 'jobid' in args:
            print(str(scr_env.resmgr.job_id()), end='')
        if 'endtime' in args:
            print(str(scr_env.resmgr.end_time()), end='')
        if 'nodes' in args:
            nodelist = scr_env.get_scr_nodelist()
            if nodelist is None:
                nodelist = scr_env.resmgr.job_nodes()
            print(str(nodelist), end='')
        if 'down' in args:
            print(str(scr_env.resmgr.down_nodes()), end='')
        if 'runnodes' in args:
            print(str(scr_env.get_runnode_count()), end='')
