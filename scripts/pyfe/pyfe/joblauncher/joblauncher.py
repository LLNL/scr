#! /usr/bin/env python3
"""
joblauncher/joblauncher.py

JobLauncher is the super class for the job launcher family.
The clustershell_exec method is provided by this class, other methods should be overridden.
If the constant, USE_CLUSTERSHELL, is not '0' and the module ClusterShell is available
then ClusterShell.Task will be available and the clustershell_exec method can be used.

Attributes
----------
launcher          - string representation of the launcher
hostfile          - string location of the hostfile, set in scr_run.py: scr_env.get_prefix() + '/.scr/hostfile'
clustershell_task - Either False or a pointer to the module ClusterShell.Task

Methods
-------
init()
    This method initializes class attributes.
prepareforprerun()
    This method is called (without arguments) before scr_prerun.py.
    Any necessary preamble work can be inserted into this method.
    This method does nothing by default and may be overridden when needed.

launchruncmd(up_nodes, down_nodes, launcher_args)
    Job launchers should override this method.
    up_nodes and down_nodes are strings with comma separated lists of nodes.
    launcher_args is a list of arguments representing the command.
    Format an argv according to the job launcher specifications.
    This method should return runproc(argv=argv, wait=False).
    The caller will receive the tuple: (subprocess.Popen object, object.pid).
parallel_exec(argv, runnodes, use_dshbak)
    Job launchers should override this method.
    argv is a list of arguments representing the command.
    runnodes is a comma separated string of nodes which will execute the command.
    use_dshbak is True by default, and determines whether the command is piped to dshbak.
    `which pdsh` and `which dshbak` are defined in ../scr_const.py as PDSH_EXE and DSHBAK_EXE.
    If self.clustershell_task is not False:
      return self.clustershell_exec(argv, runnodes, use_dshbak).
    Otherwise:
      Format pdshcmd as a list using scr_const.PDSH_EXE, the argv, and runnodes.
      If use_dshbak is False:
        return runproc(argv=pdshcmd, getstdout=True, getstderr=True).
      If use_dshbak is true:
        Let pdshcmd = [pdshcmd, [scr_const.DSHBAK_EXE, 'c']].
        return pipeproc(argv=pdshcmd, getstdout=True, getstderr=True).
scavenge_files(prog, upnodes, downnodes_spaced, cntldir, dataset_id, prefixdir, buf_size, crc_flag)
    Job launchers should override this method.
    prog is a string and is the location of the scr_copy program.
    upnodes is a string with comma separated lists of nodes.
    downnodes_spaced is a space separated string of down nodes.
    cntldir is a string for the control directory path, obtained from SCR_Param.
    dataset_id is a string for the dataset id.
    prefixdir is a string for the prefix directory path.
    buf_size is a string, set by $SCR_FILE_BUF_SIZE.
      If undefined, the default value is (1024 * 1024).
    crc_flag is a string, set by $SCR_CRC_ON_FLUSH.
      If undefined, the default value is '--crc'.
      If '0', crc_flag will be set to ''.
    Format an argv according to the job launcher specifications.
    Return self.parallel_exec(argv=argv, runnodes=upnodes, use_dshbak=False)[0]

clustershell_exec(argv, runnodes, use_dshbak)
    This method implementation is provided by the base class.
    argv is a list of arguments representing the command.
    runnodes is a comma separated string of nodes which will execute the command.
    use_dshbak is True by default, and determines the format of the returned output.
    The command specified by argv will be ran on the nodes specified by runnodes.
    The return value is a list: [output, returncode]
    The output is a list: [stdout, stderr]
    The full return value is then: [[stdout, stderr], returncode]

"""

from pyfe import scr_const


class JobLauncher(object):
  def __init__(self, launcher=''):
    self.launcher = launcher
    self.hostfile = ''
    self.watchprocess = False
    self.clustershell_task = False
    if scr_const.USE_CLUSTERSHELL != '0':
      try:
        import ClusterShell.Task as MyCSTask
        self.clustershell_task = MyCSTask
      except:
        pass

  def prepareforprerun(self):
    """Called before scr_prerun

    Gives a job launcher an opportunity to perform any preamble work

    Returns
    -------
    None
    """
    pass

  def launchruncmd(self, up_nodes='', down_nodes='', launcher_args=[]):
    """Launch job specified by launcher_args using up_nodes and down_nodes.

    Returns
    -------
    tuple (Popen object, int)
        Returns the same value returned by scr_common.runproc(argv, wait=False)
        returns (process, process id)
        or (None, -1) on error
    """
    return None, -1

  #####
  #### Return the output as pdsh / dshbak would have (?)
  # https://clustershell.readthedocs.io/en/latest/api/Task.html
  # clustershell exec can be called from any sub-resource manager
  # the sub-resource manager is responsible for ensuring clustershell is available
  ### TODO: different ssh programs may need different parameters added to remove the 'tput: ' from the output
  def clustershell_exec(self, argv=[], runnodes='', use_dshbak=True):
    task = self.clustershell_task.task_self()
    # launch the task
    task.run(' '.join(argv), nodes=runnodes)
    ret = [['', ''], 0]
    # ensure all of the tasks have completed
    self.clustershell_task.task_wait()
    # iterate through the task.iter_retcodes() to get (return code, [nodes])
    # to get msg objects, output must be retrieved by individual node using task.node_buffer or .key_error
    # retrieved outputs are bytes, convert with .decode('utf-8')
    if use_dshbak:
      # all outputs in each group are the same
      for rc, keys in task.iter_retcodes():
        if rc != 0:
          ret[1] = 1
        # groups may have multiple nodes with identical output, use output of the first node
        output = task.node_buffer(keys[0]).decode('utf-8')
        if len(output) != 0:
          ret[0][0] += '---\n'
          ret[0][0] += ','.join(keys) + '\n'
          ret[0][0] += '---\n'
          lines = output.split('\n')
          for line in lines:
            if line != '' and line != 'tput: No value for $TERM and no -T specified':
              ret[0][0] += line + '\n'
        output = task.key_error(keys[0]).decode('utf-8')
        if len(output) != 0:
          ret[0][1] += '---\n'
          ret[0][1] += ','.join(keys) + '\n'
          ret[0][1] += '---\n'
          lines = output.split('\n')
          for line in lines:
            if line != '' and line != 'tput: No value for $TERM and no -T specified':
              ret[0][1] += line + '\n'
    else:
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
  # returns [ [ stdout, stderr ] , returncode ]
  def parallel_exec(self, argv=[], runnodes='', use_dshbak=True):
    return [['', ''], 0]

  # perform the scavenge files operation for scr_scavenge
  # command format depends on resource manager in use
  # uses either pdsh or clustershell
  # returns a list -> [ 'stdout', 'stderr' ]
  def scavenge_files(self,
                     prog='',
                     upnodes='',
                     downnodes_spaced='',
                     cntldir='',
                     dataset_id='',
                     prefixdir='',
                     buf_size='',
                     crc_flag=''):
    return ['', '']

  def killsprocess(self):
    """Indicates whether a job launcher implements
        get_jobstep_id and scr_kill_jobstep
    """
    return self.watchprocess

  def get_jobstep_id(self, user='', allocid='', pid=-1):
    """Return an identifier for the most recently launched task.
    Parameters
    ----------
    str
        user: The user name from the environment
    str
        allocid: Identifier for the allocation
    int
        pid: The PID returned from parallel_exec

    Returns
    -------
    str
        jobstep id
        or None if unknown or error
    """
    return None

  def scr_kill_jobstep(self, jobstepid=None):
    """Kills task identified by jobid parameter.
        method defined when a joblauncher has a special
        procedure to kill a launched job

    """
    pass
