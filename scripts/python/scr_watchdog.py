#! /usr/bin/env python

# scr_watchdog.py

# This is a generic utility for detecting when an application that uses
# SCR is hung. It periodically checks the flush file to see if checkpointing
# activity has occurred since the last time it checked. If too much time
# passes without activity, it kills the job

import time, scr_const, sys
from datetime import datetime
from scr_param import SCR_Param
from scr_kill_jobstep import scr_kill_jobstep
from scr_common import getconf

def print_usage(prog):
  print('')
  print('  Usage:  '+prog+' [--dir <prefixDir>] [--jobStepId <jobStepId>')
  print('')

def scr_watchdog(argv):
  bindir = scr_const.X_BINDIR
  prog = 'scr_watchdog'
  scr_flush_file = 'scr_flush_file'

  # lookup timeout values from environment
  param = SCR_Param()
  timeout = None
  timeout_pfs = None

  # we have two timeout variables now, one for the length of time to wait under
  # "normal" circumstances and one for the length of time to wait if writing
  # to the parallel file system
  param_timeout = param.get('SCR_WATCHDOG_TIMEOUT')
  if param_timeout is not None:
    timeout = param_timeout;

  param_timeout_pfs = param.get('SCR_WATCHDOG_TIMEOUT_PFS')
  if param_timeout_pfs is not None:
    timeout_pfs = param_timeout_pfs

  # TODO: What to do if timeouts are not set? die? should we set default values?
  # for now die with error message

  start_time = datetime.now()

  # read in command line arguments
  conf = getconf(argv,{'-d':'prefixdir','--dir':'prefixdir','-j':'jobstepid','--jobStepId':'jobstepid'})
  if conf is None:
    print_usage(prog)
    return 1

  # check that we have a  dir and apid
  if 'jobstepid' not in conf or 'prefixdir' not in conf:
    print_usage()
    return 1
  if timeout is None or timeout_pfs is None:
    print('Necessary environment variables not set: SCR_HANG_TIMEOUT and SCR_HANG_TIMEOUT_PFS')
    return 1

  prefix    = conf['prefixdir']
  jobstepid = conf['jobstepid']

  # loop periodically checking the flush file for activity
  lastCheckpoint    = ''
  lastCheckpointLoc = ''

  getLatestCmd    = 'scr_flush_file --dir '+prefix+' -l'
  getLatestLocCmd = 'scr_flush_file --dir '+prefix+' -L'

  killCmd = 'scr_kill_jobstep '+jobstepid

  timeToSleep = int(timeout)

  while True:
    time.sleep(timeToSleep)
    #print "was sleeping, now awake\n";
    latest = getLatestCmd
    #print "latest was $latest\n";
    latestLoc = ''
    if latest!='':
      argv = [getLatestLocCmd,latest]
      runproc = subprocess.Popen(args=argv, bufsize=1, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
      latestLoc = runproc.communicate()[0]
    #print "latestLoc was $latestLoc\n";
    if latest == lastCheckpoint:
      if latestLoc == lastCheckpointLoc:
        #print "time to kill\n";
        break
    lastCheckpoint = latest;
    lastCheckpointLoc = latestLoc;
    if latestLoc == 'SYNC_FLUSHING':
      timeToSleep = int(timeout_pfs)
    else:
      timeToSleep = int(timeout)

  print('Killing simulation using '+killCmd)
  scr_kill_jobstep(killCmd)
  return 0


if __name__=='__main__':
  ret = scr_watchdog(sys.argv[2:])
  print('scr_watchdog returned '+str(ret))

