#! /usr/bin/env python

# scr_kill_jobstep.py

# This script uses the scancel command to kill the job step with the 
# job step id supplied on the command line

import sys
import scr_const
from scr_common import getconf, runproc

def print_usage(prog):
  print('')
  print('  Usage:  $prog -j <jobstepid>')
  print('')
  print('    -j, --jobStepId    The job step id to kill.')
  print('')

def scr_kill_jobstep(argv):
  prog = 'scr_kill_jobstep'
  bindir = scr_const.X_BINDIR

  killCmd = 'scancel'

  # read in the command line options
  conf = getconf(argv,{'-j':'jobid','--jobStepId':'jobid'})
  if conf is None:
    print_usage(prog)
    return 1
  if 'jobid' not in conf:
    print('You must specify the job step id to kill.')
    return 1
  jobid=conf['jobid']

  cmd = killCmd+' '+jobid
  print(cmd)
  argv = [killCmd,jobid]
  returncode = runproc(argv=argv)[1]
  return returncode

if __name__=='__main__':
  ret = scr_kill_jobstep(sys.argv[1:])
  print('scr_kill_jobstep returned '+str(ret))
