# Test the Watchdog
# This script emulates the same instructions as scr_run/watchdog
# REQUIRED: sleeper.c to be compiled with an MPI C compiler
# REQUIRED: arguments: <launcher> <launcher_args>
# 'sleeper' will be launched in the same manner as scr_run
# The sleeper program outputs to file the posix time every 5 seconds
# This script will kill the launched process after some elapsed time
# After killing the launched process this script will sleep
# If the processes were killed then the time elapsed should be greater
# than some threshhold since the last file output

import os
import sys
import time
from subprocess import TimeoutExpired

from scrjob.jobenv import JobEnv
from scrjob.watchdog import Watchdog


def checkfiletimes():
    good = True
    i = 0
    while True:
        try:
            filename = 'outrank' + str(i)
            with open(filename, 'r') as infile:
                contents = infile.readlines()
                # the 'last' line will be blank
                lastline = contents[-2].strip()
                try:
                    timeval = int(lastline)
                    elapsed = int(time.time()) - timeval
                    print('Rank', str(i), 'elapsed time:', str(elapsed))
                    if elapsed < 45:
                        good = False
                except Exception as e:
                    print(e)
                    print('Error converting \"' + lastline +
                          '\" to an integer.')
            i += 1
        except:
            print('Unable to open file for rank', str(i))
            print('(this is expected when the rank goes out of bounds.)')
            break
    return good


def testwatchdog(launcher, launcher_args):
    timeout = 15
    os.environ['SCR_WATCHDOG_TIMEOUT'] = str(timeout)
    os.environ['SCR_WATCHDOG_TIMEOUT_PFS'] = str(timeout)

    jobenv = JobEnv(launcher=launcher)

    prefix = jobenv.dir_prefix()
    nodelist = jobenv.resmgr.job_nodes()
    down_nodes = jobenv.resmgr.down_nodes()

    watchdog = Watchdog(prefix, jobenv)

    if down_nodes is None:
        down_nodes = {}

    print('Nodelist = ' + str(nodelist))
    print('Down nodes = ' + str(down_nodes))
    down_nodes = list(down_nodes.keys())

    print('Launching command ' + ' '.join(launcher_args))
    proc, jobstep = jobenv.launcher.launch_run(launcher_args,
                                               nodes=nodelist,
                                               down_nodes=down_nodes)

    if proc is None or jobstep is None:
        print('Error launching the sleeper process!')
        return

    #if watchdog is None: proc.communicate(timeout=None)
    #else:

    print(
        'Each launched sleeper process will output the posix time every 5 seconds.'
    )
    print('Calling watchdog watchprocess . . .')
    if watchdog.watchproc(proc, jobstep) != 0:
        print('The watchdog failed to start')
        print('Waiting for the original process for 15 seconds')
        (finished, success) = jobenv.launcher.wait_run(proc=proc,
                                                       timeout=timeout)
        if finished == True:
            print(
                'The process is still running, asking the launcher to kill it . . .'
            )
            jobenv.launcher.kill_run(jobstep=jobstep)

    print('The process has now been terminated')
    print('Sleeping for 45 seconds before checking the output files . . .')
    time.sleep(45)
    print('Examining files named in the manner \"outrank%d\"')
    if checkfiletimes():
        print('The results of the test appear good')
    else:
        print('It appears the processes may still be running')

    print('Watchdog test concluded')


if __name__ == '__main__':
    # requiring a launcher at a minimum, perhaps without arguments.
    if len(sys.argv) < 2:
        print('ERROR: Invalid usage')
        print('Usage: test_watchdog.py <launcher> <launcher_args>')
        print('   Ex: test_watchdog.py srun -n1 -N1')
        sys.exit(1)
    testwatchdog(sys.argv[1], sys.argv[2:])
