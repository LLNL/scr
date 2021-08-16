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
parallel_exec(argv, runnodes)
    Job launchers should override this method.
    argv is a list of arguments representing the command.
    runnodes is a comma separated string of nodes which will execute the command.
    `which pdsh` is defined in ../scr_const.py as PDSH_EXE.
    If self.clustershell_task is not False:
      return self.clustershell_exec(argv, runnodes).
    Otherwise:
      Format pdshcmd as a list using scr_const.PDSH_EXE, the argv, and runnodes.
      return runproc(argv=pdshcmd, getstdout=True, getstderr=True).
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
    Return self.parallel_exec(argv=argv, runnodes=upnodes)[0]

clustershell_exec(argv, runnodes)
    This method implementation is provided by the base class.
    argv is a list of arguments representing the command.
    runnodes is a comma separated string of nodes which will execute the command.
    The command specified by argv will be ran on the nodes specified by runnodes.
    The return value is a list: [output, returncode]
    The output is a list: [stdout, stderr]
    The full return value is then: [[stdout, stderr], returncode]

"""

import os
from pyfe import scr_const
from pyfe.scr_common import interpolate_variables

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
    # attempt to take over launchruncmd+getjobstepid+killjobstep
    self.flux = None
    if scr_const.USE_FLUX == '1':
      try:
        import flux as myflux
        self.flux = {}
        # we were able to import flux
        self.flux['flux'] = myflux
        self.flux['f'] = myflux.Flux()
        #import json (?)
        from flux.job import JobspecV1 as myjobspec
        self.flux['JobspecV1'] = myjobspec
        from flux.job.JobID import JobID as myJobID
        self.flux['JobID'] = myJobID
        ### move function definitions here to joblauncher.py from flux.py
        self.launchruncmd = self.launchfluxcmd
        self.get_jobstep_id = self.get_flux_jobstep
        self.scr_kill_jobstep = self.flux_kill_jobstep
      except Exception as e:
        print('JobLauncher: flux option was set but unable to use flux:')
        print(e)
        self.flux = None

  def waitonprocess(self, proc, timeout=None):
    if self.flux is None:
      proc.communicate(timeout=timeout)
    else:
      if timeout is not None:
        # when the timeout is set we want to ignore the TimeoutError exception
        try:
          self.flux['flux'].job.wait_async(self.flux['f'], proc).wait_for(int(timeout))
        except TimeoutError:
          # this is expected when the wait times out and it is still running
          pass
        except Exception as e:
          # it can also throw an exception if there is no job to wait for
          print(e)
      else:
        # wait without a timeout
        self.flux['flux'].job.wait_async(self.flux['f'], proc)

  def parsefluxargs(self, launcher_args):
    # default values if none are specified in launcher_args
    nnodes = 1
    ntasks = 1
    corespertask = 1
    argv = []
    for arg in launcher_args:
      if arg == 'mini' or arg == 'submit':
        pass
      elif arg.startswith('--nodes'):
        nnodes = int(arg.split('=')[1])
      elif arg.startswith('--ntasks'):
        ntasks = int(arg.split('=')[1])
      elif arg.startswith('--cores-per-task'):
        corespertask = int(arg.split('=')[1])
      else:
        argv.append(arg)
    return nnodes, ntasks, corespertask, argv

  def launchfluxcmd(self, up_nodes='', down_nodes='', launcher_args=[]):
    if type(launcher_args) is str:
      launcher_args = launcher_args.split(' ')
    nnodes, ntasks, ncores, argv = self.parsefluxargs(launcher_args)
    compute_jobreq = self.flux['JobspecV1'].from_command(
        command=argv, num_tasks=ntasks, num_nodes=nnodes, cores_per_task=ncores)
    ###
    compute_jobreq.cwd = os.getcwd()
    compute_jobreq.environment = dict(os.environ)

    #### An error I got assigning something else to stdout:
    #
    #File "... /flux/python3 /flux/job/Jobspec.py", line 360, in stdout
    #self._set_io_path("output", "stdout", path)
    #File "... /flux/python3 /flux/job/Jobspec.py", line 395, in _set_io_path
    #"The path must be a string or pathlib object, "
    ###
    #
    # Providing a path for the stdout to this variable wrote the stdout
    # from all processes (lines interleaved)
    # I think I have seen another option somewhere to prepend rank/nodenum
    #
    filepath = interpolate_variables('~/scr/install/bin/pyfe/tests/stdout')
    # all tasks will write their stdout to this file
    compute_jobreq.stdout = filepath
    job = self.flux['flux'].job.submit(self.flux['f'],
                                       compute_jobreq,
                                       waitable=True)
    # job is an integer representing the job id, this is all we need
    return job, job

  def get_flux_jobstep(self, user='', allocid='', pid=-1):
    # the incoming 'pid' is actually the jobstep id that is needed
    return pid

  def flux_kill_jobstep(self, jobstepid=None):
    if jobstepid is not None:
      try:
        self.flux['flux'].job.cancel(self.flux['f'], jobstepid)
        self.flux['flux'].job.wait_async(self.flux['f'], jobstepid)
      except Exception as e:
        # we could get invalid jobstep id when the job has already terminated
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
  # https://clustershell.readthedocs.io/en/latest/api/Task.html
  # clustershell exec can be called from any sub-resource manager
  # the sub-resource manager is responsible for ensuring clustershell is available
  ### TODO: different ssh programs may need different parameters added to remove the 'tput: ' from the output
  def clustershell_exec(self, argv=[], runnodes=''):
    task = self.clustershell_task.task_self()
    # launch the task
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
  # returns [ [ stdout, stderr ] , returncode ]
  def parallel_exec(self, argv=[], runnodes=''):
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
