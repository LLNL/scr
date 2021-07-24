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
import pyfe
from pyfe.resmgr import AutoResourceManager
from pyfe.joblauncher import AutoJobLauncher

def testwatchdog(launcher, launcher_args):
  rm = AutoResourceManager()
  nodelist = rm.get_job_nodes()
  down_nodes = rm.get_downnodes()
  launcher = AutoJobLauncher(launcher)
  launcher_args += ' ./sleeper'

  print('Nodelist = ' + str(nodelist))
  print('Down nodes = ' + str(down_nodes))

  proc, pid = launcher.launchruncmd(up_nodes=nodelist,
                                    down_nodes=down_nodes,
                                    launcher_args=launcher_args)
  if proc is None:
    print('Error launching the sleeper process!')
    sys.exit(0)

  print('Each launched sleeper process will output the posix time every 5 seconds.')
  print('Allowing the sleeper processes to run for 20 seconds . . .')
  try:
    proc.communicate(timeout=20)
  except TimeoutExpired as e:
    print('TimeoutExpired exception was caught, this is expected.')

  if proc.returncode is not None:
    print('The process has a return code, this should not have happened.')
    print('The return code is:', str(proc.returncode))
    return

  print('The process has no return code (it is still running).')
  print('This is expected, killing the process now.')
  proc.kill()
  print('Calling proc communicate() as recommended.')
  output = proc.communicate()
  print('The process now has the return code:', str(proc.returncode))
  print('The stdout should be blank: \"' + str(output[0]) + '\"')
  print('The stderr should be blank: \"' + str(output[1]) + '\"')
  print('Sleeping for 20 seconds before checking the output files . . .')
  time.sleep(20)
  print('Examining files named in the manner \"outrank%d\"')
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
        except Exception as e:
          print(e)
          print('Error converting \"' + lastline + '\" to an integer.')
      i += 1
    except:
      print('Unable to open file for rank', str(i))
      print('This is expected when the rank goes out of bounds.')
      print('This concludes the watchdog test.')
      break

if __name__ == '__main__':
  if len(sys.argv)!=3:
    print('ERROR: Invalid usage')
    print('Usage: test_watchdog.py <launcher> \"<launcher_args>\"')
    print('   Ex: test_watchdog.py srun \"-n1 -N1\"')
    sys.exit(1)
  testwatchdog(sys.argv[1], sys.argv[2])
