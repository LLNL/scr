# The flux launcher args:
# --nodes=2 --ntasks=2 --cores-per-task=1

import os
import sys

from scrjob import config
from scrjob.postrun import postrun
from scrjob.list_down_nodes import list_down_nodes
from scrjob.prerun import prerun
from scrjob.watchdog import Watchdog
from scrjob.jobenv import JobEnv


def dolaunch(launcher, launch_cmd):
    verbose = True

    bindir = config.X_BINDIR

    jobenv = JobEnv(launcher=launcher)
    prefix = jobenv.dir_prefix()

    jobid = jobenv.resmgr.job_id()
    user = jobenv.user()
    jobenv.launcher.hostfile = os.path.join(jobenv.dir_scr(), 'hostfile')
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
    else:
        print('there were downnodes returned from list_down_nodes')
        print('down_nodes = ' + str(down_nodes))
        # print the reason for the down nodes, and log them
        # when reason == True a string formatted for printing will be returned
        printstring = list_down_nodes(jobenv,
                                      nodes=nodelist,
                                      free=True,
                                      reason=True,
                                      log=None,
                                      secs='0')
        print(printstring)

    print('testing: Launching ' + str(launch_cmd))
    proc, jobstep = jobenv.launcher.launch_run(launch_cmd,
                                               nodes=nodelist,
                                               down_nodes=down_nodes)
    print('type(proc) = ' + str(type(proc)) + ', type(jobstep) = ' +
          str(type(jobstep)))
    print('proc = ' + str(proc))
    print('pid = ' + str(jobstep))

    print('testing : Entering watchdog method')
    success = False
    if watchdog.watchproc(proc, jobstep) != 0:
        print('watchdog.watchproc returned nonzero')
        print('calling launcher.wait_run . . .')
        (finished, success) = jobenv.launcher.wait_run(proc)
    else:
        print('watchdog returned zero and didn\'t launch another process')
        print('this means the process is terminated')
    print(
        f'Process has finished or has been terminated with success == {success}.'
    )


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage: ' + sys.argv[0] + ' <launcher> <launcher_args>')
        sys.exit(0)
    dolaunch(sys.argv[1], sys.argv[2:])
