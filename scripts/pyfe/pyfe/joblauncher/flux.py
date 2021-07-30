#! /usr/bin/env python3

import os
from time import sleep

#from pyfe import scr_const
#from pyfe.scr_common import runproc, pipeproc
from pyfe.joblauncher import JobLauncher
from pyfe import scr_glob_hosts

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
    self.flux = flux.Flux()

  def parsefluxargs(self, launcher_args):
    # default values if none are specified in launcher_args
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
    nnodes, ntasks, ncores, argv = self.parsefluxargs(launcher_args)
    compute_jobreq = JobspecV1.from_command(
        command=argv, num_tasks=ntasks, num_nodes=nnodes, cores_per_task=ncores)
    ###
    compute_jobreq.cwd = os.getcwd()
    compute_jobreq.environment = dict(os.environ)
    job = flux.job.submit(self.flux, compute_jobreq, waitable=True)
    ### this just returned the integer job id.
    ### need a reference to the submitted job.
    return job, job

  def parallel_exec(self, argv=[], runnodes='', use_dshbak=True):
    if type(argv) is str:
      argv = argv.split(' ')
    ### without a reference to resmgr we can't use clustershell.nodeset
    ### glob hosts instead will use scr_hostlist.expand
    ### could add a node count to this method.
    numnodes = scr_glob_hosts(count=True, hosts=runnodes)
    # we will N tasks on N nodes, 1 core per task.
    compute_jobreq = JobspecV1.from_command(
        command=argv, num_tasks=numnodes, num_nodes=numnodes, cores_per_task=1)
    compute_jobreq.cwd = os.getcwd()
    compute_jobreq.environment = dict(os.environ)
    # there is also a submit_async that returns the future
    #   that would combine the submit+wait_async into one call
    job = flux.job.submit(self.flux, compute_jobreq, waitable=True)
    future = flux.job.wait_async(self.flux, job)
    status = future.get_status()
    stderr = status.errstr.decode('UTF-8')
    retcode = 1 if status.success != 'True' else 0
    #### Need to find out how to get the stdout/stderr
    # I launched the test job with output forwarded to a log
    # on a test program did fprintf(stdout,...), couldn't see the output anywhere
    # or we'll need to use pdsh/dshbak.
    # since these commands vary between launchers, perhaps flux should be a sub-subclass
    # the stderr here wasn't from the actual program's output (fprintf(stderr,...))
    ###
    return [['', stderr], retcode]

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
      ### need a reference to the job, or to get the job from the jobid (?)
      flux.job.cancel(self.flux, jobstepid)
      flux.job.wait_async(self.flux, jobstepid)
