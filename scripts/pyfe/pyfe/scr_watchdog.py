#! /usr/bin/env python3

# scr_watchdog.py

# This is a generic utility for detecting when an application that uses
# SCR is hung. It periodically checks the flush file to see if checkpointing
# activity has occurred since the last time it checked. If too much time
# passes without activity, it kills the job

import os, sys

if 'pyfe' not in sys.path:
  sys.path.insert(0,'/'.join(os.path.realpath(__file__).split('/')[:-2]))
  import pyfe

import argparse, time
#from datetime import datetime
from pyfe import scr_const
from pyfe.scr_param import SCR_Param
from pyfe.resmgr import AutoResourceManager
from pyfe.scr_common import runproc

def scr_watchdog(prefix=None,jobstepid=None,scr_env=None):
  # check that we have a  dir and apid
  if prefix is None or jobstepid is None:
    return 1

  bindir = scr_const.X_BINDIR
  param = None
  resmgr = None
  if scr_env is not None:
    param = scr_env.param
    resmgr = scr_env.resmgr

  # lookup timeout values from environment
  if param is None:
    param = SCR_Param()
  if resmgr is None:
    resmgr = AutoResourceManager()

  # we have two timeout variables now, one for the length of time to wait under
  # "normal" circumstances and one for the length of time to wait if writing
  # to the parallel file system
  timeout = param.get('SCR_WATCHDOG_TIMEOUT')
  timeout_pfs = param.get('SCR_WATCHDOG_TIMEOUT_PFS')

  # TODO: What to do if timeouts are not set? die? should we set default values?
  # for now die with error message

  # start_time = datetime.now() ## this is not used?

  if timeout is None or timeout_pfs is None:
    print('Necessary environment variables not set: SCR_HANG_TIMEOUT and SCR_HANG_TIMEOUT_PFS')
    return 1

  timeout = int(timeout)
  timeout_pfs = int(timeout_pfs)

  # loop periodically checking the flush file for activity
  lastCheckpoint    = ''
  lastCheckpointLoc = ''

  getLatestCmd    = bindir+'/scr_flush_file --dir '+prefix+' -l'
  getLatestLocCmd = bindir+'/scr_flush_file --dir '+prefix+' -L'

  timeToSleep = timeout

  while True:
    time.sleep(timeToSleep)
    argv = getLatestCmd.split(' ')
    latest = runproc(argv=argv,getstdout=True)[0]
    latestLoc = ''
    if latest!='':
      argv = getLatestLocCmd.split(' ')
      argv.extend(latest.split(' ')[0])
      latestLoc = runproc(argv=argv,getstdout=True)[0]
    if latest == lastCheckpoint:
      if latestLoc == lastCheckpointLoc:
        # print('time to kill')
        break
    lastCheckpoint = latest
    lastCheckpointLoc = latestLoc
    if latestLoc == 'SYNC_FLUSHING':
      timeToSleep = timeout_pfs
    else:
      timeToSleep = timeout

  print('Killing simulation using scr_kill_jobstep --jobStepId '+str(jobstepid))
  resmgr.scr_kill_jobstep(jobid=jobstepid)
  return 0

if __name__=='__main__':
  parser = argparse.ArgumentParser(add_help=False, argument_default=argparse.SUPPRESS, prog='scr_watchdog')
  parser.add_argument('-h','--help', action='store_true', help='Show this help message and exit.')
  parser.add_argument('-d','--dir', metavar='<prefixDir>', type=str, default=None, help='Specify the prefix directory.')
  parser.add_argument('-j','--jobStepId', metavar='<jobStepId>', type=str, default=None, help='Specify the jobstep id.')
  args = vars(parser.parse_args())
  if 'help' in args:
    parser.print_help()
  elif args['dir'] is None or args['jobStepId'] is None:
    print('Prefix directory and job step id must be specified.')
  else:
    ret = scr_watchdog(prefix=args['prefix'],jobstepid=args['jobStepId'])
    print('scr_watchdog returned '+str(ret))
