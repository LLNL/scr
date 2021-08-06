#! /usr/bin/env python3

import os, re
from time import sleep, time

#from pyfe import scr_const
from pyfe.scr_common import scr_prefix
from pyfe.joblauncher import JobLauncher
from pyfe.scr_glob_hosts import scr_glob_hosts

# flux imports
try:
  import flux
  #import json
  from flux.job import JobspecV1
  from flux.job.JobID import JobID
except:
  pass


class FLUX(JobLauncher):
  def __init__(self, launcher='flux'):
    super(FLUX, self).__init__(launcher=launcher)
    # Don't enable Popen.terminate() for the flux parallel exec
    self.watchprocess = True
    # connect to the running Flux instance
    try:
      self.flux = flux.Flux()
    except:
      raise ImportError('Error importing flux, ensure that the flux daemon is running.')

  def waitonprocess(self, proc, timeout=None):
    if timeout is not None:
      # waiting throws a TimeoutError exception when process still running
      try:
        flux.job.wait_async(self.flux, proc).wait_for(int(timeout))
      except TimeoutError:
        # this is expected when the wait times out and it is still running
        pass
      except Exception as e:
        # it can also throw an exception if there is no job to wait for
        print(e)
    else:
      # wait without a timeout
      flux.job.wait_async(self.flux, proc)

  def parsefluxargs(self, launcher_args):
    # if scr_flux.py is called these can be trimmed there.
    # if scr_run.py flux <launcher_args> is called
    #   then these will not be trimmed yet.
    #   could add this trim in scr_run's main if launcher=='flux'
    if launcher_args[0] == 'mini':
      launcher_args = launcher_args[1:]
    if launcher_args[0] == 'run' or launcher_args[0] == 'submit':
      launcher_args = launcher_args[1:]
    # default values if none are specified in launcher_args
    nodes = 1
    ntasks = 1
    corespertask = 1
    argv = []
    for arg in launcher_args:
      if arg.startswith('--nodes'):
        nodes = int(arg.split('=')[1])
      elif arg.startswith('--ntasks'):
        ntasks = int(arg.split('=')[1])
      elif arg.startswith('--cores-per-task'):
        corespertask = int(arg.split('=')[1])
      else:
        argv.append(arg)
    return nodes, ntasks, corespertask, argv

  def launchruncmd(self, up_nodes='', down_nodes='', launcher_args=[]):
    if type(launcher_args) is str:
      launcher_args = launcher_args.split(' ')
    nnodes, ntasks, ncores, argv = self.parsefluxargs(launcher_args)
    compute_jobreq = JobspecV1.from_command(
        command=argv, num_tasks=ntasks, num_nodes=nnodes, cores_per_task=ncores)
    compute_jobreq.cwd = os.getcwd()
    compute_jobreq.environment = dict(os.environ)
    job = flux.job.submit(self.flux,
                          compute_jobreq,
                          waitable=True)
    # job is an integer representing the job id, this is all we need
    return job, job

  def parallel_exec(self, argv=[], runnodes='', use_dshbak=True):
    if type(argv) is str:
      argv = argv.split(' ')
    nnodes, ntasks, ncores, argv = self.parsefluxargs(argv)
    ### Need to determine number of nodes to set nnodes and nntasks to N
    ### without specifying it is set above to just launch 1 task on 1 cpu on 1 node
    ### glob_hosts defaults to scr_hostlist.expand(hosts)
    ###  passing in the resmgr would allow use of ClusterShell.NodeSet
    ### The size is the number of ranks flux start was launched with
    nnodes = int(self.flux.attr_get("size"))
    ntasks = nnodes
    compute_jobreq = JobspecV1.from_command(
        command=argv, num_tasks=ntasks, num_nodes=nnodes, cores_per_task=ncores)
    compute_jobreq.cwd = os.getcwd()
    compute_jobreq.environment = dict(os.environ)
    compute_jobreq.setattr_shell_option("output.stdout.label", True)
    compute_jobreq.setattr_shell_option("output.stderr.label", True)
    prefix = scr_prefix()
    timestamp = str(time())
    # time will return posix timestamp like -> '1628179160.1724932'
    # some unique filename to send stdout/stderr to
    ### the script prepends the rank to stdout, not stderr.
    outfilename = 'out' + timestamp
    errfilename = 'err' + timestamp
    outfilename = os.path.join(prefix, outfilename)
    errfilename = os.path.join(prefix, errfilename)
    # all tasks will write their stdout to this file
    compute_jobreq.stdout = outfilename
    compute_jobreq.stderr = errfilename
    job = flux.job.submit(self.flux,
                          compute_jobreq,
                          waitable=True)
    future = flux.job.wait_async(self.flux, job)
    status = future.get_status()
    ret = [['', ''], 0]
    # don't fail if can't open a file, just leave output blank
    try:
      with open(outfilename,'r') as infile:
        ret[0][0] = infile.read()
      os.remove(outfilename)
    except:
      pass
    try:
      with open(errfilename,'r') as infile:
        ret[0][1] = infile.read()
    except:
      try:
        ret[0][1] = status.errstr.decode('UTF-8')
      except:
        pass
    # stderr set in a nested try, remove the errfile here
    try:
      os.remove(errfilename)
    except:
      pass
    ret[1] = 0 if status.success == True else 1
    return ret

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
    argv = [
        prog, '--cntldir', cntldir, '--id', dataset_id, '--prefix', prefixdir,
        '--buf', buf_size, crc_flag, downnodes_spaced
    ]
    output = self.parallel_exec(argv=argv, runnodes=upnodes,
                                use_dshbak=False)[0]
    return output

  # the 'pid' returned by the above launchruncmd is the jobid
  def get_jobstep_id(self, user='', allocid='', pid=-1):
    return pid

  def scr_kill_jobstep(self, jobstepid=None):
    if jobstepid is not None:
      try:
        flux.job.cancel(self.flux, jobstepid)
        flux.job.wait_async(self.flux, jobstepid)
      except Exception as e:
        # we could get 'invalid jobstep id' when the job has already terminated
        pass
