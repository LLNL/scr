#! /usr/bin/env python3

import os
import sys
from time import sleep

from scrjob import config
from scrjob.postrun import postrun
from scrjob.list_down_nodes import list_down_nodes
from scrjob.common import scr_prefix
from scrjob.prerun import prerun
from scrjob.watchdog import Watchdog
from scrjob.environment import JobEnv
from scrjob.glob_hosts import glob_hosts


# return number of nodes left in allocation after excluding down nodes
def nodes_remaining(resmgr, nodelist, down_nodes):
    num_left = glob_hosts(count=True,
                          minus=nodelist + ':' + down_nodes,
                          resmgr=resmgr)
    if num_left is None:
        return 0
    return int(num_left)


# determine how many nodes are needed
def nodes_needed(jobenv, nodelist):
    # if SCR_MIN_NODES is set, use that
    num_needed = os.environ.get('SCR_MIN_NODES')
    if num_needed is None or int(num_needed) <= 0:
        # otherwise, use value in nodes file if one exists
        num_needed = jobenv.get_runnode_count()
        if num_needed <= 0:
            # otherwise, assume we need all nodes in the allocation
            num_needed = glob_hosts(count=True,
                                    hosts=nodelist,
                                    resmgr=jobenv.resmgr)
            if num_needed is None:
                # failed all methods to estimate the minimum number of nodes
                return 0
    return int(num_needed)


def getpdshout(launcher, launch_cmd):
    verbose = True

    bindir = config.X_BINDIR

    prefix = scr_prefix()

    jobenv = SCR_Env(prefix=prefix, launcher=launcher)

    jobid = jobenv.resmgr.job_id()
    user = jobenv.get_user()
    print('jobid = ' + str(jobid))
    print('user = ' + str(user))

    # get the nodeset of this job
    nodelist = jobenv.get_scr_nodelist()
    if nodelist is None:
        nodelist = jobenv.resmgr.job_nodes()
        if nodelist is None:
            nodelist = ''
    nodelist = ','.join(jobenv.resmgr.expand_hosts(nodelist))
    print('nodelist = ' + str(nodelist))

    jobenv.resmgr.usewatchdog(True)

    watchdog = Watchdog(prefix, jobenv)
    if launcher == 'srun':
        print('calling prepare_prerun . . .')
        jobenv.launcher.prepare_prerun()
        print('returned from prepare_prerun')
    if prerun(jobenv=jobenv) != 0:
        print('testing: ERROR: Command failed: prerun -p ' + prefix)
        print('This would terminate run')
    else:
        print('prerun returned success')

    endtime = jobenv.resmgr.end_time()
    print('endtime = ' + str(endtime))

    if endtime == 0:
        print('testing : WARNING: Unable to get end time.')
    elif endtime == -1:  # no end time / limit
        print('testing : end_time returned no end time / limit')
    else:
        print('testing : end_time returned ' + str(endtime))

    down_nodes = list_down_nodes(free=True, nodeset_down='', jobenv=jobenv)
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
                                      jobenv=jobenv,
                                      log=log)
        print(printstring)

    num_needed = nodes_needed(jobenv, nodelist)
    print('num_needed = ' + str(num_needed))

    num_left = nodes_remaining(jobenv.resmgr, nodelist, down_nodes)
    print('num_left = ' + str(num_left))

    print('testing: Getting output from: ' + str(launch_cmd))
    output = jobenv.launcher.parallel_exec(argv=launch_cmd, runnodes=nodelist)
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
