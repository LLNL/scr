#! /usr/bin/env python3

# scr_get_jobstep_id.py

import os, sys

if 'pyfe' not in sys.path:
  sys.path.insert(0, '/'.join(os.path.realpath(__file__).split('/')[:-2]))
  import pyfe

from pyfe.scr_environment import SCR_Env
from pyfe.resmgr import AutoResourceManager
from pyfe.joblauncher import AutoJobLauncher

# This script attempts to get the job step id for the last srun command that
# was launched. The argument to this script is the PID of the srun command.
# The script first calls squeue to get the job steps for this job.
# It assumes that the highest numbered job step is the right one (meaning the
# last one that was started).
# Then it checks to see if the srun command with PID is still running. The idea
# here is that if the srun command died, then the job step with the highest number
# is for some other srun command and not the one we are looking for.
# This script returns the job step id on success and -1 on failure.


def scr_get_jobstep_id(scr_env=None, pid=-1):
  #my $pid=$ARGV[0]; # unused
  if scr_env is None or scr_env.resmgr is None or scr_env.launcher is None:
    return None
  user = scr_env.get_user()
  if user is None:
    print('scr_get_jobstep_id: ERROR: Could not determine user ID')
    return None
  currjobid = scr_env.launcher.get_jobstep_id(user=user, pid=pid)
  if currjobid is None:
    print('scr_get_jobstep_id: ERROR: Could not determine job ID')
    return None
  return currjobid


if __name__ == '__main__':
  if len(sys.argv) != 2:
    print('scr_get_jobstep_id: job launcher must be specified.')
    print('                Ex: scr_get_jobstep_id.py srun')
    sys.exit(1)
  scr_env = SCR_Env()
  scr_env.resmgr = AutoResourceManager()
  scr_env.launcher = AutoJobLauncher(sys.argv[1])
  ret = scr_get_jobstep_id(scr_env)
  if ret is None:
    sys.exit(1)
  print(ret)
  sys.exit(0)
