#! /usr/bin/env python3

import os, sys

sys.path.insert(0, '/'.join(os.path.realpath(__file__).split('/')[:-2]))
import scrjob

from time import sleep

from scrjob import scr_const
from scrjob.postrun import postrun
from scrjob.list_down_nodes import list_down_nodes
from scrjob.scr_common import scr_prefix
from scrjob.scr_prerun import scr_prerun
from scrjob.scr_watchdog import SCR_Watchdog
from scrjob.scr_environment import SCR_Env
from scrjob.launchers import AutoJobLauncher
from scrjob.resmgrs import AutoResourceManager
from scrjob.scr_param import SCR_Param
from scrjob.scr_glob_hosts import scr_glob_hosts


# return number of nodes left in allocation after excluding down nodes
def nodes_remaining(resmgr, nodelist, down_nodes):
    num_left = scr_glob_hosts(count=True,
                              minus=nodelist + ':' + down_nodes,
                              resmgr=resmgr)
    if num_left is None:
        return 0
    return int(num_left)


# determine how many nodes are needed
def nodes_needed(scr_env, nodelist):
    # if SCR_MIN_NODES is set, use that
    num_needed = os.environ.get('SCR_MIN_NODES')
    if num_needed is None or int(num_needed) <= 0:
        # otherwise, use value in nodes file if one exists
        num_needed = scr_env.get_runnode_count()
        if num_needed <= 0:
            # otherwise, assume we need all nodes in the allocation
            num_needed = scr_glob_hosts(count=True,
                                        hosts=nodelist,
                                        resmgr=scr_env.resmgr)
            if num_needed is None:
                # failed all methods to estimate the minimum number of nodes
                return 0
    return int(num_needed)


def getpdshout(launcher, launch_cmd):
    verbose = True
    bindir = scr_const.X_BINDIR
    prefix = scr_prefix()
    param = SCR_Param()
    resmgr = AutoResourceManager()
    launcher = AutoJobLauncher(launcher)
    scr_env = SCR_Env(prefix=prefix)
    scr_env.param = param
    scr_env.resmgr = resmgr
    scr_env.launcher = launcher
    jobid = resmgr.job_id()
    user = scr_env.get_user()
    print('jobid = ' + str(jobid))
    print('user = ' + str(user))
    # get the nodeset of this job
    nodelist = scr_env.get_scr_nodelist()
    if nodelist is None:
        nodelist = resmgr.job_nodes()
        if nodelist is None:
            nodelist = ''
    nodelist = ','.join(resmgr.expand_hosts(nodelist))
    print('nodelist = ' + str(nodelist))
    resmgr.usewatchdog(True)
    watchdog = SCR_Watchdog(prefix, scr_env)
    if launcher == 'srun':
        print('calling prepare_prerun . . .')
        launcher.prepare_prerun()
        print('returned from prepare_prerun')
    if scr_prerun(scr_env=scr_env) != 0:
        print('testing: ERROR: Command failed: scr_prerun -p ' + prefix)
        print('This would terminate run')
    else:
        print('prerun returned success')
    endtime = resmgr.end_time()
    print('endtime = ' + str(endtime))
    if endtime == 0:
        print('testing : WARNING: Unable to get end time.')
    elif endtime == -1:  # no end time / limit
        print('testing : end_time returned no end time / limit')
    else:
        print('testing : end_time returned ' + str(endtime))
    down_nodes = list_down_nodes(free=True, nodeset_down='', scr_env=scr_env)
    if type(down_nodes) is int:
        print('there were no downnodes from list_down_nodes')
        down_nodes = ''
    else:
        print('there were downnodes returned from list_down_nodes')
        print('down_nodes = ' + str(down_nodes))
        # print the reason for the down nodes, and log them
        # when reason == True a string formatted for printing will be returned
        printstring = list_down_nodes(reason=True,
                                      free=first_run,
                                      nodeset_down=down_nodes,
                                      runtime_secs='0',
                                      scr_env=scr_env,
                                      log=log)
        print(printstring)
    num_needed = nodes_needed(scr_env, nodelist)
    print('num_needed = ' + str(num_needed))
    num_left = nodes_remaining(resmgr, nodelist, down_nodes)
    print('num_left = ' + str(num_left))
    print('testing: Getting output from: ' + str(launch_cmd))
    output = launcher.parallel_exec(argv=launch_cmd, runnodes=nodelist)
    print('###\n# stdout:')
    print(output[0][0])
    print('###\n# stderr:')
    print(output[0][1])
    print('###\n# Execution return code: ' + str(output[1]))
    print('\nTest pdshout concluded.')
    sleep(2)


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage: ' + sys.argv[0] + ' <launcher> <launcher_args>')
        sys.exit(0)
    getpdshout(sys.argv[1], sys.argv[2:])
