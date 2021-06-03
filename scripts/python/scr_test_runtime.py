#! /usr/bin/env python

#scr_test_runtime.py

import subprocess, sys

bindir = "@X_BINDIR@"
prog = "scr_test_runtime"

# Checks for pdsh and dshbak
# returns 0 if OK, returns 1 if a command not found
def scr_test_runtime:
  pdsh = "@PDSH_EXE@"
  dshbak = "@DSHBAK_EXE@"
  #pdsh = 'pdsh'
  # assume we won't find any problem
  rc = 0

  # check that we have pdsh
  argv=['which',pdsh]
  runproc = subprocess.Popen(args=argv)
  runproc.communicate()
  if runproc.returncode!=0:
    print(prog+': ERROR: \'which '+pdsh+'\' failed')
    print(prog+': ERROR: Problem using pdsh, see README for help')
    rc = 1

  # check that we have dshbak
  argv[1]=dshbak
  runproc = subprocess.Popen(args=argv)
  runproc.communicate()
  if runproc.returncode!=0:
    print(prog+': ERROR: \'which '+dshbak+'\' failed')
    print(prog+': ERROR: Problem using dshbak, see README for help')
    rc = 1

  return rc
