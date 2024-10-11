"""Module defining JobLauncher base class."""
import sys

from subprocess import TimeoutExpired


class JobLauncher:
    """JobLauncher is the super class for the job launcher family.

    Methods
    -------
    Methods whose functionality is not provided or compatible must be overridden.

    launch_run() should be overridden, and should return a tuple of 2 values.
    The first value will be passed to wait_run().
    The second value will be passed to kill_run().

    launch_run() may return scr_common.runproc(argv, wait=False) and use the
    provided wait_run() and kill_run() methods

    wait_run() returns a tuple of 2 boolean values: (completed, success)

    Default methods
    ---------------
    wait_run(), kill_run()
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

    def launch_run(self, args, nodes=None, down_nodes=None):
        """This method is called to launch a jobstep.

        This method must be overridden.
        Launch a jobstep specified by launcher_args using up_nodes and down_nodes.

        Returns
        -------
        tuple (process, id)
            When using the provided base class methods: wait_run() and kill_run(),
            return scr_common.runproc(argv, wait=False).

            The first return value is used as the argument for waiting on the process in launcher.wait_run().
            The second return value is used as the argument to kill a jobstep in launcher.kill_run().
        """
        return None, None

    def wait_run(self, proc=None, timeout=None):
        """This method is called after a jobstep is launched with the return
        values of launch_run.

        The base class method may be used when launch_run returns runproc(argv=argv,wait=False).
        Override this implementation in any base class whose launch_run returns a different value,
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
                out, err = proc.communicate(timeout=timeout)
            except TimeoutExpired:
                return (False, None)
            except Exception as exc:
                print(f'wait_run for proc {proc} failed with exception {exc}')
                return (None, None)
            else:
                if out:
                    print(out)
                if err:
                    print(err, file=sys.stderr)
        return (True, proc.returncode)

    def kill_run(self, jobstep=None):
        """Kills task identified by jobstep parameter.

        When launcher.launch_run() returns scr_common.runproc(argv,
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
            except Exception:
                pass
