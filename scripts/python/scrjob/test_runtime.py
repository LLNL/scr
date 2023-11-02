import os
import sys

from scrjob import config
from scrjob.common import runproc


class TestRuntime:
    """Contains methods to determine whether we should launch with SCR.

    This class contains methods to test the environment to ensure SCR will be able to function.
    These tests are called in scr_prerun.py, immediately after checking if SCR is enabled.
    Not all methods are appropriate to test in every environment.
    Additional test methods may be added to this class to integrate into the pre-run tests.
    Test methods should run without arguments and return None on success or raise an exception on error.

    We could receive a list of applicable tests through:
      A configuration file.
      A compile constant (config.py).
      Environment variables.
      Querying the ResourceManager and/or Joblauncher classes.

    Each test in the list, that is a callable method in this class, will be called during scr_prerun.
    If any indicated test fails then scr_prerun will fail.

    This class is currently a 'static' class, not intended to be instantiated.
    The __new__ method is used to launch the tests, this method returns the success value.
    Methods other than `__new__` should be decorated with `@staticmethod`.
    """

    def __new__(cls):
        """This method collects the return codes of all static methods.

        Returns
        -------
          This method returns None on success and raises an Exception on error.
          This method does not return an instance of a class.
        """

        tests = []

        if config.USE_CLUSTERSHELL:
            tests.append('check_clustershell')
        else:
            tests.append('check_pdsh')

        for test in tests:
            testmethod = getattr(TestRuntime, test)
            testmethod()

    @staticmethod
    def check_clustershell():
        """This method tests if the ClusterShell module is available.

        This test will return failure if the option USE_CLUSTERSHELL
        enables ClusterShell and the module is not available.
        """
        try:
            import ClusterShell
        except:
            raise RuntimeError(
                'Failed to import Clustershell as indicated by config.py')

    @staticmethod
    def check_pdsh():
        """This method tests the validity of the pdsh command.

        The pdsh executable, whose path is determined by
        config.PDSH_EXE, is used as a RemoteExec method to execute a
        command on multiple nodes and retrieve each node's output and
        return code.

        This test will return failure if we are unable to confirm
        PDSH_EXE is valid.
        """

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
        pdsh = config.PDSH_EXE
        argv = ['which', pdsh]
        rc = runproc(argv=argv, shell=True)[1]
        if rc != 0:
            raise RuntimeError(f'Failed to find pdsh {pdsh}')


if __name__ == '__main__':
    """Call sys.exit(result) When this is called as a standalone script.

    This provides a compact illustration of usage of the test methods in
    scr_prerun.
    """
    try:
        TestRuntime()
    except:
        sys.exit(1)
