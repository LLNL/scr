#! /usr/bin/env python3

# Test the scr_watchdog
# This script emulates the same instructions as scr_run/watchdog
# REQUIRED: sleeper.c to be compiled with an MPI C compiler
# REQUIRED: arguments: <launcher> <launcher_args>
# 'sleeper' will be launched in the same manner as scr_run
# The sleeper program outputs to file the posix time every 5 seconds
# This script will kill the launched process after some elapsed time
# After killing the launched process this script will sleep
# If the processes were killed then the time elapsed should be greater
# than some threshhold since the last file output

import os, sys
import time
from subprocess import TimeoutExpired

sys.path.insert(0, '/'.join(os.path.realpath(__file__).split('/')[:-2]))
import scrjob
from scrjob.resmgrs import AutoResourceManager
from scrjob.launchers import AutoJobLauncher
from scrjob.scr_param import SCR_Param
from scrjob.scr_environment import SCR_Env
from scrjob.scr_watchdog import SCR_Watchdog


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
    os.environ['SCR_WATCHDOG_TIMEOUT'] = '15'
    os.environ['SCR_WATCHDOG_TIMEOUT_PFS'] = '15'
    scr_env = SCR_Env()
    param = SCR_Param()
    rm = AutoResourceManager()
    launcher = AutoJobLauncher(launcher)
    prefix = scr_env.get_prefix()
    scr_env.param = param
    scr_env.launcher = launcher
    nodelist = rm.get_job_nodes()
    down_nodes = rm.get_downnodes()
    watchdog = SCR_Watchdog(prefix, scr_env)

    if down_nodes is None:
        down_nodes = {}

    print('Nodelist = ' + str(nodelist))
    print('Down nodes = ' + str(down_nodes))
    down_nodes = list(down_nodes.keys())

    print('Launching command ' + ' '.join(launcher_args))
    proc, jobstep = launcher.launchruncmd(up_nodes=nodelist,
                                          down_nodes=down_nodes,
                                          launcher_args=launcher_args)

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
        (finished, success) = launcher.waitonprocess(proc=proc,
                                                     timeout=timeout)
        if finished == True:
            print(
                'The process is still running, asking the launcher to kill it . . .'
            )
            launcher.scr_kill_jobstep(jobstep=jobstep)

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
