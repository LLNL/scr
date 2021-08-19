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

if 'pyfe' not in sys.path:
  sys.path.insert(0, '/'.join(os.path.realpath(__file__).split('/')[:-2]))
  import pyfe

from pyfe import scr_const
from pyfe.scr_common import runproc

### TODO: The tests performed should be configurable,
### as with the tests in the resmgr/nodetests.py Nodetests class.

class SCR_Test_Runtime:
  """SCR_Test_Runtime class contains methods to determine whether we should launch with SCR

  This class is currently a 'static' class, not intended to be instantiated.
  This class contains static methods, each of which perform one test.

  Currently, when using the scr_test_runtime() method or this script with __name__ == '__main__',
  all methods are iterated through, and a failure is returned if any individual test fails.

  Test Return Values
  ------------------
  Each test should return an integer:
  0 - success
  1 - failure
  """
  @staticmethod
  def check_clustershell():
    """This method tests if the ClusterShell module is available
    
    This test will return failure if the option USE_CLUSTERSHELL
    enables ClusterShell and the module is not available.
    """
    if scr_const.USE_CLUSTERSHELL == '1':
      try:
        import ClusterShell
      except:
        print(
            'scr_test_runtime: ERROR: Unable to import Clustershell as indicated by scr_const.py'
        )
        return 1
    return 0

  @staticmethod
  def check_pdsh():
    """This method tests the validity of the pdsh command

    The pdsh executable, whose path is determined by scr_const.PDSH_EXE,
    is used in the Joblauncher.parallel_exec method to execute a command
    in parallel on multiple nodes and retrieve each node's output and return code.

    This test will return failure if we are unable to confirm PDSH_EXE is valid.

    If ClusterShell is enabled, this test does not apply.
    """
    if scr_const.USE_CLUSTERSHELL == '1':
      return 0
    ### TODO: If the Flux joblauncher is in use, this test does not apply.
    pdsh = scr_const.PDSH_EXE
    ### Could change to ['pdsh', '-V']
    ### or some other command?
    argv = ['which', pdsh]
    returncode = runproc(argv=argv)[1]
    ### TODO: Validate pdsh command through some other means
    ### or determine appropriate return values.
    # From subprocess.Popen docs, it appears Python 3.59+ all return the same values.
    # Return value of None indicates the program is still running (it will not be with the above call)
    # A negative return value (POSIX only) indicates the process was terminated by that signal.
    # (  -9 indicates the process received signal 9  )
    # Otherwise, the return value should be the return value of the process.
    # This should typically be 0 for success, and nonzero for failure
    if returncode != 0:
      print('scr_test_runtime: ERROR: \'which ' + pdsh + '\' failed')
      print('scr_test_runtime: ERROR: Problem using pdsh, see README for help')
      return 1
    return 0


def scr_test_runtime():
  """This method collects the return codes of all methods declared in SCR_Test_Runtime

  This method returns 0 if all tests succeed.
  Otherwise this method returns 1, indicating a test has failed.
  """
  ### TODO: We are currently iterating through all tests in the class
  ### This needs to be a configurable subset of the class methods
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
  """When this script is called as main, simply call sys.exit(scr_test_runtime())"""
  ret = scr_test_runtime()
  sys.exit(ret)