#! /usr/bin/env python3

# scr_test_runtime.py

import os, sys

if 'pyfe' not in sys.path:
  sys.path.insert(0, '/'.join(os.path.realpath(__file__).split('/')[:-2]))
  import pyfe

from pyfe import scr_const
from pyfe.scr_common import runproc
from pyfe.resmgr import AutoResourceManager

### TODO: Configuration of which test methods to use

class SCR_Test_Runtime:
  """SCR_Test_Runtime class contains methods to determine whether we should launch with SCR

  This class contains methods to test the environment, to ensure SCR will be able to function.
  These tests are called in scr_prerun.py, immediately after checking if scr is enabled.
  Not all methods are appropriate to test in every environment.
  Additional test methods may be added to this class to integrate into the pre-run tests.
  Test methods should run without arguments and return an integer representing PASS / FAIL.

  We could receive a list of applicable tests through:
    A configuration file.
    A compile constant (scr_const.py).
    Environment variables.
    Querying the ResourceManager and/or Joblauncher classes.

  Currently the ResourceManager is queried, which aligns with the original bash/perl scripts.

  Each test in the list, that is a callable method in this class, will be called during scr_prerun.
  If any indicated test fails then scr_prerun will fail.

  This class is currently a 'static' class, not intended to be instantiated.
  The __new__ method is used to launch the tests, this method returns the success value.
  Methods other than `__new__` should be decorated with `@staticmethod`.

  Test Return Values
  ------------------
  Each test should return an integer:
  0 - PASS
  1 - FAIL
  """
  def __new__(cls, tests=[]):
    """This method collects the return codes of all static methods declared in SCR_Test_Runtime

    This method receives a list.
      Each element is a string and is the name of a method of the SCR_Test_Runtime class

    Returns
    -------
      This method returns an integer.
      This method does not return an instance of a class.

      This method returns 0 if all tests succeed,
      Otherwise this method returns the 1, indicating a test has failed.
    """
    #tests = [
    #    attr for attr in dir(SCR_Test_Runtime) if not attr.startswith('__')
    #    and callable(getattr(SCR_Test_Runtime, attr))
    #]
    rc = []
    for test in tests:
      try:
        testmethod = getattr(SCR_Test_Runtime, test)
        if callable(testmethod):
          rc.append(testmethod())
        else:
          print('SCR_Test_Runtime: ERROR: ' + test + ' is defined but is not a test method.')
      except AttributeError as e:
        print('SCR_Test_Runtime: ERROR: ' + test + ' is not defined.')
        print('dir(SCR_Test_Runtime)='+str(dir(SCR_Test_Runtime)))
      except Exception as e:
        # Could set an error code . . .
        print('scr_test_runtime: ERROR: Exception in test ' + test)
        print(e)
    return 1 if 1 in rc else 0

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
    pdsh = scr_const.PDSH_EXE
    ### TODO: Validate pdsh command through some other means for some resmgrs?
    ### there was an issue reported with this.
    # I added a parameter to runproc, shell=False (default False)
    #  when passing shell=True to runproc it will shlex.quote the command
    #  it will turn ['which', 'pdsh'] into -> "bash -c \"which pdsh\""
    #  this is what the command looked like in the original scripts
    # This worked for me in both SLURM and LSF.
    ###
    # From subprocess.Popen docs, it appears Python 3.59+ all return the same values.
    # Return value of None indicates the program is still running (it will not be with the above call)
    # A negative return value (POSIX only) indicates the process was terminated by that signal.
    # (  -9 indicates the process received signal 9  )
    # Otherwise, the return value should be the return value of the process.
    # This should typically be 0 for success, and nonzero for failure
    ### This test may not work everywhere
    argv = ['which', pdsh]
    returncode = runproc(argv=argv, shell=True)[1]
    if returncode != 0:
      print('scr_test_runtime: ERROR: \'which ' + pdsh + '\' failed')
      print('scr_test_runtime: ERROR: Problem using pdsh, see README for help')
      return 1
    return 0


if __name__ == '__main__':
  """Call sys.exit(result) When this is called as a standalone script

  This provides a compact illustration of usage of the test methods in scr_prerun.
  """
  resmgr = AutoResourceManager()
  tests = resmgr.get_prerun_tests()
  ret = SCR_Test_Runtime(tests)
  sys.exit(ret)
