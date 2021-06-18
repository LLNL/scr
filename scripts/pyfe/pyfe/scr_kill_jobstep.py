#! /usr/bin/env python

# scr_kill_jobstep.py

# This script uses the scancel command to kill the job step with the 
# job step id supplied on the command line

import argparse
import scr_const
from scr_common import runproc

def scr_kill_jobstep(bindir=None,jobid=None):
  if bindir is None:
    bindir = scr_const.X_BINDIR

  killCmd = 'scancel'

  if jobid is None:
    return 1

  cmd = killCmd+' '+str(jobid)
  print(cmd)
  argv = [killCmd,jobid]
  returncode = runproc(argv=argv)[1]
  return returncode

if __name__=='__main__':
  parser = argparse.ArgumentParser(add_help=False, argument_default=argparse.SUPPRESS, prog='scr_kill_jobstep')
  parser.add_argument('-h','--help', action='store_true', help='Show this help message and exit.')
  parser.add_argument('-b','--bindir', metavar='<bindir>', default=None, help='Specify the bin directory.')
  parser.add_argument('-j','--jobStepId', metavar='<jobstepid>', type=str, help='The job step id to kill.')
  args = vars(parser.parse_args())
  if 'help' in args:
    parser.print_help()
  elif 'jobStepId' not in args:
    print('You must specify the job step id to kill.')
  else:
    ret = scr_kill_jobstep(bindir=args['bindir'],jobid=args['jobStepId'])
    if ret == 0:
      print('Kill command issued successfully.')
    else:
      print('Kill command returned with code '+str(ret))

