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
import multiprocessing as mp

sys.path.insert(0, '/'.join(os.path.realpath(__file__).split('/')[:-2]))
import pyfe
from pyfe.resmgr import AutoResourceManager
from pyfe.joblauncher import AutoJobLauncher
from pyfe.scr_param import SCR_Param
from pyfe.scr_environment import SCR_Env
from pyfe.scr_watchdog import SCR_Watchdog

def checkfiletimes():
  good = True
  i = 0
  while True:
    try:
      filename = 'outrank' + str(i)
      with open(filename,'r') as infile:
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
          print('Error converting \"' + lastline + '\" to an integer.')
      i += 1
    except:
      print('Unable to open file for rank', str(i))
      print('This is expected when the rank goes out of bounds.')
      print('This concludes the watchdog test.')
      break
  return good

def testwatchdog(launcher, launcher_args):
  mp.set_start_method('fork')
  os.environ['SCR_WATCHDOG_TIMEOUT'] = '1'
  os.environ['SCR_WATCHDOG_TIMEOUT_PFS'] = '1'
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
    down_nodes = ''
  launcher_args.append('./sleeper')

  print('Nodelist = ' + str(nodelist))
  print('Down nodes = ' + str(down_nodes))

  proc, pid = launcher.launchruncmd(up_nodes=nodelist,
                                    down_nodes=down_nodes,
                                    launcher_args=launcher_args)

  if proc is None:
    print('Error launching the sleeper process!')
    return

  #if watchdog is None: proc.communicate(timeout=None)
  #else:

  print('Each launched sleeper process will output the posix time every 5 seconds.')
  print('Allowing the sleeper processes to run for 15 seconds . . .')
  time.sleep(15)
  print('Calling watchdog watchprocess')
  if watchdog.watchproc(proc) != 0:
    print('The watchdog failed to start')
    print('Waiting for the original process to terminate now . . .')
    proc.communicate(timeout=None)
  elif watchdog.process is not None:
    print('The watchdog launched a separate process will be launched for')
    print('  joblaunchers that require a specific method to kill a launched job')
    print('Waiting for that process to terminate now . . .')
    watchdog.process.join()
    jobstepid = launcher.get_jobstep_id()
    print('jobstep id is now ' + str(jobstepid))

  print('The process has now been terminated')
  print('Sleeping for 45 seconds before checking the output files . . .')
  time.sleep(45)
  print('Examining files named in the manner \"outrank%d\"')
  if checkfiletimes():
    print('The results of the test appear good')
  else:
    print('It appears the processes are still running')

  print('Watchdog test concluded')


if __name__ == '__main__':
  if len(sys.argv)!=3:
    print('ERROR: Invalid usage')
    print('Usage: test_watchdog.py <launcher> <launcher_args>')
    print('   Ex: test_watchdog.py srun -n1 -N1')
    sys.exit(1)
  testwatchdog(sys.argv[1], sys.argv[2:])
