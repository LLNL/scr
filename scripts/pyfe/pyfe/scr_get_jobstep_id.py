#! /usr/bin/env python3

# scr_get_jobstep_id.py

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

def scr_get_jobstep_id(scr_env=None,pid=-1):
  #my $pid=$ARGV[0]; # unused
  if scr_env is None:
    scr_env = SCR_Env()
  if scr_env.resmgr is None:
    scr_env.resmgr = AutoResourceManager()
  if scr_env.launcher is None:
    scr_env.launcher = AutoJobLauncher()
  user = scr_env.conf['user']
  if user is None:
    print('scr_get_jobstep_id: ERROR: Could not determine user ID')
    return None
  jobid = scr_env.resmgr.conf['jobid']
  if jobid is None:
    print('scr_get_jobstep_id: ERROR: Could not determine job ID')
    return None
  currjobid = scr_env.launcher.get_jobstep_id(user=user,pid=pid)
  return currjobid

if __name__=='__main__':
  ret = scr_get_jobstep_id()
  if ret is not None:
    print(str(ret))

