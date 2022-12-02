#! @Python_EXECUTABLE@

import io, os, re, argparse
from time import sleep, time

from scrjob.scr_common import scr_prefix
from scrjob.launchers import JobLauncher
from scrjob.scr_glob_hosts import scr_glob_hosts

# flux imports
try:
  import flux
  #import json
  from flux.job import JobspecV1
  from flux.job.JobID import JobID
  from flux.resource import ResourceSet
  from flux.rpc import RPC
except:
  pass


class FLUX(JobLauncher):
  def __init__(self, launcher='flux'):
    super(FLUX, self).__init__(launcher=launcher)
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
        # return 1 to indicate the process is still running and timeout has expired
        return 1
      except Exception as e:
        # it can also throw an exception if there is no job to wait for
        print(e)
    else:
      # wait without a timeout
      future = flux.job.wait_async(self.flux, proc)
      status = future.get_status()
    # return 0 to indicate the program is no longer running (or there was some exception)
    return 0

  def parsefluxargs(self, launcher_args):
    parser = argparse.ArgumentParser( prog = 'launcher/flux.py',
      description = 'launch scr enabled applications under flux')

    # These arguments are discarded.  We must name them here so they are
    # not included in "remaining" which collects the user script and args.
    parser.add_argument('mini', choices=['mini'])
    parser.add_argument('subcmd', choices=['run','submit'])

    # flux interface from_command() requires we specify these values
    # so we must extract them from the args provided by the user
    parser.add_argument('-N', '--nodes', default=1, type=int)
    parser.add_argument('-n', '--ntasks', default=1, type=int)
    parser.add_argument('-c', '--cores-per-task', default=1, type=int)
    parser.add_argument('-g', '--gpus-per-task', default=0, type=int)
    parser.add_argument('-x', '--exclusive', action='store_true')

    # depends on python >= 3.7
    args, remaining = parser.parse_known_intermixed_args(launcher_args)

    return args.nodes, args.ntasks, args.cores_per_task, args.gpus_per_task, args.exclusive, remaining

  def launchruncmd(self, up_nodes='', down_nodes='', launcher_args=[]):
    if type(launcher_args) is str:
      launcher_args = launcher_args.split(' ')
    nnodes, ntasks, ncores, ngpus, excl, argv = self.parsefluxargs(launcher_args)
    compute_jobreq = JobspecV1.from_command(command=argv,
                                            num_tasks=ntasks,
                                            num_nodes=nnodes,
                                            cores_per_task=ncores,
                                            gpus_per_task=ngpus,
                                            exclusive=excl)
    compute_jobreq.cwd = os.getcwd()
    compute_jobreq.environment = dict(os.environ)
    job = flux.job.submit(self.flux,
                          compute_jobreq,
                          waitable=True)
    # job is an integer representing the job id, this is all we need
    return job, job

  def parallel_exec(self, argv=[], runnodes=''):
    if type(argv) is str:
      argv = argv.split(' ')
    nnodes, ntasks, ncores, argv = self.parsefluxargs(argv)
    ### Need to determine number of nodes to set nnodes and nntasks to N
    ### without specifying it is set above to just launch 1 task on 1 cpu on 1 node
    ### glob_hosts defaults to scr_hostlist.expand(hosts)
    ###  passing in the resmgr would allow use of ClusterShell.NodeSet
    ### The size is the number of ranks flux start was launched with
    resp = RPC(self.flux, 'resource.status').get()
    rset = ResourceSet(resp['R'])
    nnodes = rset.nnodes
    ntasks = nnodes
    compute_jobreq = JobspecV1.from_command(
        command=argv, num_tasks=ntasks, num_nodes=nnodes, cores_per_task=ncores)
    ### create a yaml 'file' stream from a string to get JobspecV1.from_yaml_stream()
    #   This may allow to explicitly specify the nodes to run on
    # string = 'yaml spec'
    # stream = io.StringIO(string)
    # compute_jobreq = JobspecV1.from_yaml_stream(stream)
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
    # get the hostlist to swap ranks for hosts
    nodelist = rset.nodelist
    future = flux.job.wait_async(self.flux, job)
    status = future.get_status()
    ret = [['', ''], 0]
    # don't fail if can't open a file, just leave output blank
    try:
      with open(outfilename,'r') as infile:
        lines = infile.readlines()
        for line in lines:
          try:
            rank = re.search('\d', line)
            host = nodelist[int(rank[0])]
            line = host + line[line.find(':'):]
          except:
            pass
          ret[0][0] += line
      os.remove(outfilename)
    except:
      pass
    try:
      with open(errfilename,'r') as infile:
        lines = infile.readlines()
        for line in lines:
          try:
            rank = re.search('\d', line)
            host = nodelist[int(rank[0])]
            line = host + line[line.find(':'):]
          except:
            pass
          ret[0][1] += line
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

  def scr_kill_jobstep(self, jobstep=None):
    if jobstep is not None:
      try:
        flux.job.cancel(self.flux, jobstep)
        flux.job.wait_async(self.flux, jobstep)
      except Exception:
        # we could get 'invalid jobstep id' when the job has already terminated
        pass
