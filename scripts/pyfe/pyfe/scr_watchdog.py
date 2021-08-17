#! /usr/bin/env python3

# scr_watchdog.py

# This is a generic utility for detecting when an application that uses
# SCR is hung. It periodically checks the flush file to see if checkpointing
# activity has occurred since the last time it checked. If too much time
# passes without activity, it kills the job

from pyfe import scr_const
from pyfe.scr_param import SCR_Param
from pyfe.cli import SCRFlushFile

class SCR_Watchdog:
  def __init__(self,prefix,scr_env):
    # we have two timeout variables now, one for the length of time to wait under
    # "normal" circumstances and one for the length of time to wait if writing
    # to the parallel file system
    self.timeout = scr_env.param.get('SCR_WATCHDOG_TIMEOUT')
    self.timeout_pfs = scr_env.param.get('SCR_WATCHDOG_TIMEOUT_PFS')

    if self.timeout is not None and self.timeout_pfs is not None:
      self.timeout = int(self.timeout)
      self.timeout_pfs = int(self.timeout_pfs)

    self.launcher = scr_env.launcher

    # interface to query values from the SCR flush file
    self.scr_flush_file = SCRFlushFile(prefix)

  def watchfiles(self, proc, jobstep):
    # loop periodically checking the flush file for activity
    timeToSleep = self.timeout
    lastCheckpoint = None
    lastCheckpointLoc = None
    while True:
      # wait up to 'timeToSleep' to see if the process terminates normally
      if self.launcher.waitonprocess(proc, timeout=timeToSleep) == 0:
        # when the wait returns zero the process is no longer running
        return 0

      # the process is still running, read flush file to get latest
      # dataset id and its location
      latestLoc = None
      latest = self.scr_flush_file.latest()
      if latest:
        latestLoc = self.scr_flush_file.location(latest)

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
        timeToSleep = self.timeout_pfs
      else:
        timeToSleep = self.timeout
    # forward progress not observed in an expected timeframe
    # kill the watched process and return
    self.launcher.scr_kill_jobstep(jobstep)
    return 0

  def watchproc(self, watched_process=None, jobstep=None):
    if watched_process is None or jobstep is None:
      print('scr_watchdog: ERROR: No process to watch.')
      return 1

    # TODO: What to do if timeouts are not set? die? should we set default values?
    # for now die with error message
    if self.timeout is None or self.timeout_pfs is None:
      print(
          'Necessary environment variables not set: SCR_WATCHDOG_TIMEOUT and SCR_WATCHDOG_TIMEOUT_PFS'
      )
      # Returning 1 to scr_run to indicate watchdog did not start/complete
      return 1
    return self.watchfiles(proc=watched_process, jobstep=jobstep)
