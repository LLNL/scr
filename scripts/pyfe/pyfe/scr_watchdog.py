#! /usr/bin/env python3

# scr_watchdog.py

# This is a generic utility for detecting when an application that uses
# SCR is hung. It periodically checks the flush file to see if checkpointing
# activity has occurred since the last time it checked. If too much time
# passes without activity, it kills the job

import os, sys

if 'pyfe' not in sys.path:
  sys.path.insert(0, '/'.join(os.path.realpath(__file__).split('/')[:-2]))
  import pyfe

from subprocess import TimeoutExpired

from pyfe.scr_param import SCR_Param
from pyfe.cli import SCRFlushFile


def scr_watchdog(prefix=None, watched_process=None, scr_env=None):
  # check that we have a dir and a process
  if prefix is None:
    print('scr_watchdog: ERROR: Prefix must be specified.')
    return 1
  if watched_process is None:
    print('scr_watchdog: ERROR: No process to watch.')
    return 1

  # interface to query values from the SCR flush file
  scr_flush_file = SCRFlushFile(prefix)

  # we'll lookup timeout values from environment
  param = None
  if scr_env is not None:
    param = scr_env.param
  if param is None:
    param = SCR_Param()

  # we have two timeout variables now, one for the length of time to wait under
  # "normal" circumstances and one for the length of time to wait if writing
  # to the parallel file system
  timeout = param.get('SCR_WATCHDOG_TIMEOUT')
  timeout_pfs = param.get('SCR_WATCHDOG_TIMEOUT_PFS')

  # TODO: What to do if timeouts are not set? die? should we set default values?
  # for now die with error message
  if timeout is None or timeout_pfs is None:
    print(
        'Necessary environment variables not set: SCR_HANG_TIMEOUT and SCR_HANG_TIMEOUT_PFS'
    )
    return 1

  timeout = int(timeout)
  timeout_pfs = int(timeout_pfs)

  # loop periodically checking the flush file for activity
  timeToSleep = timeout
  lastCheckpoint = None
  lastCheckpointLoc = None
  while True:
    # wait up to 'timeToSleep' to see if the process terminates normally
    try:
      watched_process.communicate(timeout=timeToSleep)

      # the process has terminated normally, leave the watchdog method
      return 0
    except TimeoutExpired as e:
      pass

    # the process is still running, read flush file to get latest
    # dataset id and its location
    latestLoc = None
    latest = scr_flush_file.latest()
    if latest:
      latestLoc = scr_flush_file.location(latest)

    # If nothing has changed since we last checked,
    # assume the process is hanging and kill it.
    if latest == lastCheckpoint:
      if latestLoc == lastCheckpointLoc:
        # print('time to kill')
        break

    # The flush file has changed since we last checked.
    # Assume the process is still making progress.
    # Remember current values, set new timeout, and loop back to wait again.
    lastCheckpoint = latest
    lastCheckpointLoc = latestLoc
    if latestLoc == 'SYNC_FLUSHING':
      timeToSleep = timeout_pfs
    else:
      timeToSleep = timeout

  # forward progress not observed in an expected timeframe
  # kill the watched process and return
  if watched_process.returncode is None:
    print('Killing simulation PID ' + str(watched_process.pid))
    watched_process.kill()
    watched_process.communicate()
  return 0
