#! /usr/bin/env python

#scr_test_runtime.py

import subprocess, sys
import scr_const

# Checks for pdsh and dshbak
# returns 0 if OK, returns 1 if a command not found
def scr_test_runtime():
  bindir = scr_const.X_BINDIR
  prog = 'scr_test_runtime'
  pdsh = scr_const.PDSH_EXE
  dshbak = scr_const.DSHBAK_EXE
  # assume we won't find any problem
  rc = 0

  # check that we have pdsh
  argv=['which',pdsh]
  runproc = subprocess.Popen(args=argv, bufsize=1, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
  runproc.communicate()
  if runproc.returncode!=0:
    print(prog+': ERROR: \'which '+pdsh+'\' failed')
    print(prog+': ERROR: Problem using pdsh, see README for help')
    rc = 1

  # check that we have dshbak
  argv[1]=dshbak
  runproc = subprocess.Popen(args=argv, bufsize=1, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
  runproc.communicate()
  if runproc.returncode!=0:
    print(prog+': ERROR: \'which '+dshbak+'\' failed')
    print(prog+': ERROR: Problem using dshbak, see README for help')
    rc = 1

  return rc

if __name__=='__main__':
  ret = scr_test_runtime()
  print('scr_test_runtime returned '+str(ret))

