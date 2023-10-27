import os
from subprocess import TimeoutExpired
from scrjob import scr_const
from scrjob.scr_common import interpolate_variables


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
    clustershell_exec()
      If the constant, USE_CLUSTERSHELL, is not '0', and the module ClusterShell is available,
      then the clustershell_exec method will be used instead of parallel_exec.

    waitonprocess(), kill_jobstep()
    These may both be used if the overridden launchcmd method returns scr_common.runproc(argv, wait=False).
    These each expect the Popen object returned from scr_common.runproc(wait=False).

    scavenge_files:
    If the default argv for the scavenge_files command are compatible,
    then the default scavenge_files method may be used.

    Attributes
    ----------
    launcher          - string representation of the launcher
    hostfile          - string location of a writable hostfile, set in scr_run.py: jobenv.dir_scr() + '/hostfile'
                        this is a file a launcher may use, provided for use in parallel_exec()
    clustershell_task - Either False or a pointer to the module ClusterShell.Task, if this value is not False
                        a launcher can use clustershell_exec() rather than parallel_exec().
    """

    def __init__(self, launcher=''):
        """Base class initialization.

        Call super().__init__() in derived launchers.
        """
        self.launcher = launcher
        self.hostfile = ''
        self.clustershell_task = False
        if scr_const.USE_CLUSTERSHELL != '0':
            try:
                import ClusterShell.Task as MyCSTask
                self.clustershell_task = MyCSTask
            except:
                pass

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

    #####
    # https://clustershell.readthedocs.io/en/latest/api/Task.html
    # clustershell exec can be called from any sub-resource manager
    # the sub-resource manager is responsible for ensuring clustershell is available
    ### TODO: different ssh programs may need different parameters added to remove the 'tput: ' from the output
    def clustershell_exec(self, argv=[], runnodes=[]):
        """This method implements the functionality of parallel_exec using
        clustershell.

        This method may be called from a launcher's parallel_exec if self.clustershell_task is not False.
        if self.clustershell_task is not False:
          return self.clustershell_exec(argv, runnodes)

        argv is a list of arguments representing the command.
        runnodes is a comma separated string of nodes which will execute the command.

        The command specified by argv will be ran on the nodes specified by runnodes.

        Returns
        -------
        list
          The return value is: [output, returncode],
          where output is a list: [stdout, stderr],
          so the full return value is then: [[stdout, stderr], returncode].
        """
        if runnodes:
            runnodes = self.join_hosts(runnodes)

        # launch the task
        task = self.clustershell_task.task_self()
        task.run(' '.join(argv), nodes=runnodes)
        ret = [['', ''], 0]

        # ensure all of the tasks have completed
        self.clustershell_task.task_wait()

        # iterate through the task.iter_retcodes() to get (return code, [nodes])
        # to get msg objects, output must be retrieved by individual node using task.node_buffer or .key_error
        # retrieved outputs are bytes, convert with .decode('utf-8')
        for rc, keys in task.iter_retcodes():
            if rc != 0:
                ret[1] = 1
            for host in keys:
                output = task.node_buffer(host).decode('utf-8')
                for line in output.split('\n'):
                    if line != '' and line != 'tput: No value for $TERM and no -T specified':
                        ret[0][0] += host + ': ' + line + '\n'
                output = task.key_error(host).decode('utf-8')
                for line in output.split('\n'):
                    if line != '' and line != 'tput: No value for $TERM and no -T specified':
                        ret[0][1] += host + ': ' + line + '\n'
        return ret

    # perform a generic pdsh / clustershell command
    def parallel_exec(self, argv=[], runnodes=[]):
        """Job launchers should override this method to run the command in the
        manner of pdsh.

        argv is a list of arguments representing the command.
        runnodes is a comma separated string of nodes which will execute the command.

        `which pdsh` is defined in scr_const.PDSH_EXE.
        If self.clustershell_task is not False:
          return self.clustershell_exec(argv, runnodes).
        Otherwise:
          Format pdshcmd as a list using scr_const.PDSH_EXE, the argv, and runnodes.
          return runproc(argv=pdshcmd, getstdout=True, getstderr=True).
        """
        if self.clustershell_task is not False:
            return self.clustershell_exec(argv=argv, runnodes=runnodes)
        return [['', ''], 0]

    # generate the argv to perform the scavenge files operation
    # command format depends on resource manager in use
    # returns a list -> [ 'stdout', 'stderr' ]
    def scavenge_files(self,
                       prog='',
                       nodes_up=[],
                       nodes_down=[],
                       cntldir='',
                       dataset_id='',
                       prefixdir='',
                       buf_size='',
                       crc_flag=''):
        """Job launchers may override this method to change the scavenge
        command.

        The scavenge argv is formed in this method and launched with launcher.parallel_exec().

        Arguments
        ---------
        prog               string, the location of the scr_copy program.
        nodes_up           list(string), list of up nodes.
        nodes_down         list(string), list of down nodes.
        cntldir            string, the control directory path, obtained from SCR_Param.
        dataset_id         string, the dataset id.
        prefixdir          string, the prefix directory path.
        buf_size           string, set by $SCR_FILE_BUF_SIZE.
          (If undefined, the default value is (1024 * 1024)).
        crc_flag           string, set by $SCR_CRC_ON_FLUSH.
          (If undefined, the default value is '--crc').
          (If '0', crc_flag will be set to '').
          (Otherwise, this will be the crc_flag to use).

        Formats the argv for the scavenge command.

        Returns
        -------
        list
              The text output (first element of the list) of launcher.parallel_exec()
              ['stdout', 'stderr']
        """
        downnodes = ' '.join(nodes_down)
        argv = [
            prog, '--cntldir', cntldir, '--id', dataset_id, '--prefix',
            prefixdir, '--buf', buf_size, crc_flag, downnodes
        ]
        output = self.parallel_exec(argv=argv, runnodes=nodes_up)[0]
        return output

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
