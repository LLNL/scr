#! /usr/bin/env python3

# This script wraps the scr_flush_file command.

import os

from pyfe.scr_common import runproc

class FlushFile:
  def __init__(self, bindir, prefix):
    self.bindir = bindir # path to SCR bin directory
    self.prefix = prefix # path to SCR_PREFIX
    self.flush_file = os.path.join(bindir, "scr_flush_file") + " --dir " + prefix

  # return list of output datasets
  def list_dsets_output(self):
    dsets, rc = runproc(self.flush_file + " --list-output", getstdout=True)
    if rc == 0:
      return dsets.split()

  # return list of checkpoints
  def list_dsets_ckpt(self, before=None):
    cmd = self.flush_file + " --list-ckpt"
    if before:
      cmd = cmd + " --before " + before
    dsets, rc = runproc(cmd, getstdout=True)
    if rc == 0:
      return dsets.split()

  # determine whether this dataset needs to be flushed
  def need_flush(self, d):
    rc = runproc(scr_flush_file + " --need-flush " + d)[1]
    return (rc != 0)

  # return name of specified dataset
  def name(self, d):
    name, rc = runproc(scr_flush_file + " --name " + d, getstdout=True)
    if rc == 0:
      return name
