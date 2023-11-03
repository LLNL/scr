import os
import sys
from time import sleep

from scrjob import config
from scrjob.postrun import postrun
from scrjob.list_down_nodes import list_down_nodes
from scrjob.prerun import prerun
from scrjob.watchdog import Watchdog
from scrjob.jobenv import JobEnv


def getpdshout(launcher, launch_cmd):
    verbose = True

    bindir = config.X_BINDIR

    jobenv = JobEnv(launcher=launcher)
    prefix = jobenv.dir_prefix()

    jobid = jobenv.resmgr.job_id()
    user = jobenv.user()
    print('jobid = ' + str(jobid))
    print('user = ' + str(user))

    # get the nodeset of this job
    nodelist = jobenv.node_list()
    if not nodelist:
        nodelist = jobenv.resmgr.job_nodes()
    print('nodelist = ' + str(nodelist))

    watchdog = Watchdog(prefix, jobenv)

    try:
        prerun(jobenv=jobenv)
        print('prerun returned success')
    except:
        print('testing: ERROR: Command failed: prerun -p ' + prefix)
        print('This would terminate run')

    endtime = jobenv.resmgr.end_time()
    print('endtime = ' + str(endtime))

    if endtime == 0:
        print('testing : WARNING: Unable to get end time.')
    elif endtime == -1:  # no end time / limit
        print('testing : end_time returned no end time / limit')
    else:
        print('testing : end_time returned ' + str(endtime))

    down_nodes = list_down_nodes(jobenv, nodes=nodelist, free=True)
    if not down_nodes:
        print('there were no downnodes from list_down_nodes')
        down_nodes = ''
    else:
        print('there were downnodes returned from list_down_nodes')
        print('down_nodes = ' + str(down_nodes))
        # print the reason for the down nodes, and log them
        # when reason == True a string formatted for printing will be returned
        printstring = list_down_nodes(jobenv,
                                      nodes=nodelist,
                                      free=True,
                                      reason=True,
                                      log=log,
                                      secs='0')
        print(printstring)

    print('testing: Getting output from: ' + str(launch_cmd))
    result = jobenv.rexec.rexec(launch_cmd, nodelist, jobenv)
    print('###\n# stdout:')
    for node in nodelist:
        print(node)
        print(result.stdout(node))
    print('###\n# stderr:')
    for node in nodelist:
        print(node)
        print(result.stderr(node))
    print('\nTest pdshout concluded.')
    sleep(2)


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage: ' + sys.argv[0] + ' <launcher> <launcher_args>')
        sys.exit(0)
    getpdshout(sys.argv[1], sys.argv[2:])
