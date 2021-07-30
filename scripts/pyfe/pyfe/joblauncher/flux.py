#! /usr/bin/env python3

from time import sleep

#from pyfe import scr_const
#from pyfe.scr_common import runproc, pipeproc
from pyfe.joblauncher import JobLauncher

# flux imports
try:
  import flux
  import json
  from flux.job import JobspecV1
  from flux.job.JobID import JobID
except:
  pass


class FLUX(JobLauncher):
  def __init__(self, launcher='flux'):
    super(FLUX, self).__init__(launcher=launcher)
    # not using Popen for the parallel exec
    self.watchprocess = True
    # connect to the running Flux instance
    self.flux = flux.Flux()

  def parsefluxargs(self, launcher_args):
    nodes = 1
    ntasks = 1
    corespertask = 1
    argv = []
    for arg in launcher_args:
      if arg == 'mini' or arg == 'submit':
        pass
      elif arg.startswith('--nodes'):
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
    nodes, ntasks, ncores, argv = self.parsefluxargs(launcher_args)
    compute_jobreq = JobspecV1.from_command(
        command=argv, num_tasks=ntasks, num_nodes=nnodes, cores_per_task=ncores)
    compute_jobreq.cwd = os.getcwd()
    compute_jobreq.environment = dict(os.environ)
    job = flux.job.submit(self.flux, compute_jobreq, waitable=True)
    return job, job

  # perform a generic pdsh / clustershell command
  # returns [ [ stdout, stderr ] , returncode ]
  def parallel_exec(self, argv=[], runnodes='', use_dshbak=True):
    if type(argv) is str:
      argv = argv.split(' ')
    numnodes = len(runnodes)
    compute_jobreq = JobspecV1.from_command(
        command=argv, num_tasks=numnodes, num_nodes=numnodes, cores_per_task=1)
    compute_jobreq.cwd = os.getcwd()
    compute_jobreq.environment = dict(os.environ)
    job = flux.job.submit(self.flux, compute_jobreq, waitable=True)
    future = flux.job.wait_async(self.flux, job)
    return future.status

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
      flux.job.cancel(self.flux, jobstepid)
      flux.job.wait_async(self.flux, jobstepid)
