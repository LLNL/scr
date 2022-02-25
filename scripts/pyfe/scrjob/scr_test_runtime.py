#! /usr/bin/env python3

#scr_test_runtime.py

# scr_test_runtime() is called immediately after checking if scr is enabled and setting verbosity
# the test runtime method calls every method in the SCR_Test_Runtime class
# additional tests can be added by just adding new static methods
# tests should return 0 for success and 1 for failure
# tests can be disabled by removing, commenting out, or prepending a test method's name with 2 underscores
# tests do not take arguments
# an example of declaration and use of a class variable is commented out

# alternatively, tests could be manually added and manually called
# the return value of manual additions should be appended to rc[] in scr_test_runtime
# (or handled manually as well)

import os, sys

if 'scrjob' not in sys.path:
  sys.path.insert(0, '/'.join(os.path.realpath(__file__).split('/')[:-2]))
  import scrjob

from scrjob import scr_const
from scrjob.scr_common import runproc


class SCR_Test_Runtime:
  # check that we have ClusterShell
  @staticmethod
  def check_clustershell():
    try:
      import ClusterShell
    except:
      if scr_const.USE_CLUSTERSHELL == '1':
        print(
            'scr_test_runtime: ERROR: Unable to import Clustershell as indicated by scr_const.py'
        )
        return 1
    return 0

  # check that we have pdsh
  @staticmethod
  def check_pdsh():
    ### Don't need to check this when using clustershell
    if scr_const.USE_CLUSTERSHELL == '1':
      return 0
    pdsh = scr_const.PDSH_EXE
    argv = ['which', pdsh]
    returncode = runproc(argv=argv)[1]
    if returncode != 0:
      print('scr_test_runtime: ERROR: \'which ' + pdsh + '\' failed')
      print('scr_test_runtime: ERROR: Problem using pdsh, see README for help')
      return 1
    return 0

  # check that we have dshbak
  @staticmethod
  def check_dshbak():
    ### Don't need to check this when using clustershell
    if scr_const.USE_CLUSTERSHELL == '1':
      return 0
    dshbak = scr_const.DSHBAK_EXE
    argv = ['which', dshbak]
    returncode = runproc(argv=argv)[1]
    if returncode != 0:
      print('scr_test_runtime: ERROR: \'which ' + dshbak + '\' failed')
      print(
          'scr_test_runtime: ERROR: Problem using dshbak, see README for help')
      return 1
    return 0


# collects return codes of all methods declared in SCR_Test_Runtime
# returns 0 if OK, returns 1 if any test returns 1
def scr_test_runtime():
  tests = [
      attr for attr in dir(SCR_Test_Runtime) if not attr.startswith('__')
      and callable(getattr(SCR_Test_Runtime, attr))
  ]
  rc = []
  # iterate through the methods of the SCR_Test_Runtime class
  for test in tests:
    rc.append(getattr(SCR_Test_Runtime, test)())
  return 1 if 1 in rc else 0


if __name__ == '__main__':
  ret = scr_test_runtime()
  sys.exit(ret)
