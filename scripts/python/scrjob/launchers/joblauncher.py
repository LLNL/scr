import os
from subprocess import TimeoutExpired

from scrjob import config


class JobLauncher(object):
    """JobLauncher is the super class for the job launcher family.

    Methods
    -------
    Methods whose functionality is not provided or compatible must be overridden.

    launch_run_cmd() should be overridden, and should return a tuple of 2 values.
    The first value will be passed to waitonprocess().
    The second value will be passed to kill_jobstep().

    launch_run_cmd() may return scr_common.runproc(argv, wait=False) and use the
    provided waitonprocess() and kill_jobstep() methods

    waitonprocess() returns a tuple of 2 boolean values: (completed, success)

    Default methods
    ---------------
    waitonprocess(), kill_jobstep()
    These may both be used if the overridden launchcmd method returns scr_common.runproc(argv, wait=False).
    These each expect the Popen object returned from scr_common.runproc(wait=False).

    Attributes
    ----------
    name              - string representation of the launcher
    hostfile          - string location of a writable hostfile, set in scr_run.py: jobenv.dir_scr() + '/hostfile'
                        this is a file a launcher may use
    """

    def __init__(self, launcher='UNKNOWN'):
        """Base class initialization.

        Call super().__init__() in derived launchers.
        """
        self.name = launcher
        self.hostfile = ''

    def prepare_prerun(self):
        """This method is called (without arguments) before scr_prerun.py.

        Any necessary preamble work can be inserted into this method.
        This method does nothing by default and may be overridden as
        needed.
        """
        pass

    def launch_run_cmd(self, up_nodes='', down_nodes='', launcher_args=[]):
        """This method is called to launch a jobstep.

        This method must be overridden.
        Launch a jobstep specified by launcher_args using up_nodes and down_nodes.

        Returns
        -------
        tuple (process, id)
            When using the provided base class methods: waitonprocess() and kill_jobstep(),
            return scr_common.runproc(argv, wait=False).

            The first return value is used as the argument for waiting on the process in launcher.waitonprocess().
            The second return value is used as the argument to kill a jobstep in launcher.kill_jobstep().
        """
        return None, None

    def waitonprocess(self, proc=None, timeout=None):
        """This method is called after a jobstep is launched with the return
        values of launch_run_cmd.

        The base class method may be used when launch_run_cmd returns runproc(argv=argv,wait=False).
        Override this implementation in any base class whose launch_run_cmd returns a different value,
        or if there is a different method used to wait on a process (blocking or with a timeout).
        The timeout will be none (wait until process terminates) or a numeric value indicating seconds.
        A timeout value will exist when using Watchdog.

        Returns
        -------
           A tuple containing (completed, success)
           completed:
               None -  an exception occurred
               True -  the process is no longer running
               False - we waited for the timeout and the process is still running
           success:
               None -  the job wasn't found or has not finished, no status
               True -  the process exited 0
               False - the process failed (exit code nonzero)
        """
        if proc is not None:
            try:
                proc.communicate(timeout=timeout)
            except TimeoutExpired:
                return (False, None)
            except e:
                print(
                    f'waitonprocess for proc {proc} failed with exception {e}')
                return (None, None)

        return (True, proc.returncode)

    def kill_jobstep(self, jobstep=None):
        """Kills task identified by jobstep parameter.

        When launcher.launch_run_cmd() returns scr_common.runproc(argv,
        wait=False), then this method may be used to send the terminate
        signal through the Popen object.
        """
        if jobstep is not None:
            try:
                # if jobstep.returncode is None:
                # send the SIGTERM message to the subprocess.Popen object
                jobstep.terminate()

                # ensure complete
                jobstep.communicate()
            except:
                pass
