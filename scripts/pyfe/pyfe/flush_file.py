#! /usr/bin/env python3

import os

from pyfe.scr_common import runproc

class FlushFile:
  def __init__(self, bindir, prefix):
    self.bindir = bindir # path to SCR bin directory
    self.prefix = prefix # path to SCR_PREFIX
    self.exe = os.path.join(bindir, "scr_flush_file") + " --dir " + prefix

  # return list of output datasets
  def list_dsets_output(self):
    dsets, rc = runproc(self.exe + " --list-output", getstdout=True)
    if rc == 0:
      return dsets.split()
    return []

  # return list of checkpoints
  def list_dsets_ckpt(self, before=None):
    cmd = self.exe + " --list-ckpt"
    if before:
      cmd = cmd + " --before " + before
    dsets, rc = runproc(cmd, getstdout=True)
    if rc == 0:
      return dsets.split()
    return []

  # determine whether this dataset needs to be flushed
  def need_flush(self, d):
    rc = runproc(self.exe + " --need-flush " + d)[1]
    return (rc == 0)

  # return name of specified dataset
  def name(self, d):
    name, rc = runproc(self.exe + " --name " + d, getstdout=True)
    if rc == 0:
      return name.rstrip()
