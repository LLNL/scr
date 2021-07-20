#! /usr/bin/env python3

# slurm.py
# SLURM is a subclass of ResourceManager

import os, re
import datetime

from pyfe import scr_const
from pyfe.scr_common import runproc, pipeproc
from pyfe.resmgr import nodetests, ResourceManager

# AutoResourceManager class holds the configuration

class SLURM(ResourceManager):
  # init initializes vars from the environment
  def __init__(self):
    super(SLURM, self).__init__(resmgr='SLURM')

  # get SLURM jobid of current allocation
  def getjobid(self):
    return os.environ.get('SLURM_JOBID')

  # query SLURM for most recent jobstep in current allocation
  def get_jobstep_id(self, user='', pid=-1):
    jobid = self.getjobid()
    if user == '' or jobid is None:
      return -1

    # get job steps for this user and job, order by decreasing job step
    # so first one should be the one we are looking for
    #   squeue -h -s -u $user -j $jobid -S "-i"
    # -h means print no header, so just the data in this order:
    # STEPID         NAME PARTITION     USER      TIME NODELIST
    cmd = ['squeue', '-h', '-s', '-u', user, '-j', jobid, '-S', '\"-i\"']
    output = runproc(argv=cmd, getstdout=True)[0]
    output = re.search('\d+', output)
    if output is None:
      return -1
    return output[0]

  # get node list
  def get_job_nodes(self):
    return os.environ.get('SLURM_NODELIST')

  # use sinfo to query SLURM for the list of nodes it thinks to be down
  def get_downnodes(self):
    nodelist = self.get_job_nodes()
    if nodelist is not None:
      argv = ['sinfo', '-ho', '%N', '-t', 'down', '-n', nodelist]
      down, returncode = runproc(argv=argv, getstdout=True)
      if returncode == 0:
        down = down.strip()
        return down
    return None

  def scr_kill_jobstep(self,jobid=-1):
    if jobid==-1:
      print('You must specify the job step id to kill.')
      return 1
    return runproc(argv=['scancel',str(jobid)])[1]

  # query SLURM for allocation endtime, expressed as secs since epoch
  def get_scr_end_time(self):
    # get jobid
    jobid = self.getjobid()
    if jobid is None:
      return 0

    # ask scontrol for endtime of this job
    argv = ['scontrol', '--oneliner', 'show', 'job', jobid]
    output = runproc(argv=argv, getstdout=True)[0]
    m = re.search('EndTime=(\\S*)', output)
    if not m:
      return 0

    # parse time string like "2021-07-16T14:05:12" into secs since epoch
    timestr = m.group(1)
    dt = datetime.datetime.strptime(timestr, "%Y-%m-%dT%H:%M:%S")
    timestamp = int(dt.strftime("%s"))
    return timestamp

  # return a hash to define all unavailable (down or excluded) nodes and reason
  def list_down_nodes_with_reason(self,nodes=[], scr_env=None, free=False, cntldir_string=None, cachedir_string=None):
    unavailable = nodetests.list_resmgr_down_nodes(nodes=nodes, resmgr_nodes=self.expand_hosts(self.get_downnodes()))
    nextunavail = nodetests.list_nodes_failed_ping(nodes=nodes)
    unavailable.update(nextunavail)
    if scr_env is not None and scr_env.param is not None:
      exclude_nodes = self.expand_hosts(scr_env.param.get('SCR_EXCLUDE_NODES'))
      nextunavail = nodetests.list_param_excluded_nodes(nodes=self.expand_hosts(nodes), exclude_nodes=exclude_nodes)
      unavailable.update(nextunavail)
      # assert scr_env.resmgr == self
      nextunavail = nodetests.check_dir_capacity(nodes=nodes, free=free, scr_env=scr_env, cntldir_string=cntldir_string, cachedir_string=cachedir_string)
      unavailable.update(nextunavail)
    return unavailable
