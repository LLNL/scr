#! /usr/bin/env python3

# scr_resourcemgr_lsf.py
# SCR_Resourcemgr_LSF is a subclass of SCR_Resourcemgr_Base

import os, re
from time import time
from pyfe import scr_const, scr_hostlist
from pyfe.resmgr import ResourceManager
from pyfe.scr_common import runproc
from pyfe.scr_list_down_nodes import SCR_List_Down_Nodes

class LSF(ResourceManager):
  # init initializes vars from the environment
  def __init__(self):
    super(LSF, self).__init__(resmgr='LSF')

  # no watchdog in LSF
  def usewatchdog(self,use_scr_watchdog=None):
    if use_scr_watchdog is None:
      return False
    if use_scr_watchdog==True:
      print('WARNING: SCR_WATCHDOG not supported on LSF.')

  # get job id, setting environment flag here
  def getjobid(self):
    val = os.environ.get('LSB_JOBID')
    # val may be None
    return val

  # get node list
  def get_job_nodes(self):
    val = os.environ.get('LSB_DJOB_HOSTFILE')
    if val is not None:
      try:
        lines = []
        with open(val,'r') as hostfile:
          # make a list from the set -> make a set from the list -> file.readlines().rstrip('\n')
          # get a list of lines without newlines and skip the first line
          lines = [line.strip() for line in hostfile.readlines()][1:]
          # get a set of unique hostnames, convert list to set and back
        if len(lines)==0:
          raise ValueError('Hostfile empty')
        hosts_unique = list(set(lines))
        hostlist = scr_hostlist.compress(hosts_unique)
        return hostlist
      # failed to read file
      except:
        pass
    val = os.environ.get('LSB_HOSTS')
    if val is not None:
      val = scr_hostlist.compress(val)
    # or, with jobid: squeue -j <jobid> -ho %N
    return val

  def get_downnodes(self):
    val = os.environ.get('LSB_HOSTS')
    if val is not None:
      # TODO : any way to get list of down nodes in LSF?
      pass
    return None

  def get_jobstep_id(self,user='',pid=-1):
    # previously weren't able to get jobid
    if self.conf['jobid'] is None:
      return -1
    # the slurm get_jobstep_id didn't use the pid parameter
    # get job steps for this user and job, order by decreasing job step
    # so first one should be the one we are looking for
    # -h means print no header, so just the data in this order:
    # STEPID         NAME PARTITION     USER      TIME NODELIST
    argv = ['squeue','-h','-s','-u',user,'-j',str(self.conf['jobid']),'-S','\"-i\"']
    # my $cmd="squeue -h -s -u $user -j $jobid -S \"-i\"";
    output, returncode = runproc(argv=argv,getstdout=True)
    if returncode != 0:
        return -1
    output = output.split('\n')

    currjobid=-1
    argv = ['ps','h','-p',str(pid)] if pid>=0 else []
    for line in output:
      line = line.strip()
      if len(line)==0:
        continue
      #line = re.sub('^(\s+)','',line)
      # $line=~ s/^\s+//;
      fields = re.split('\s+',line)
      # my @fields = split /\s+/, $line;
      #print ("fields ",join(",",@fields),"\n");
      #my @jobidparts=split /\./, $fields[0];
      jobidparts = fields[0].split('.')
      #print ("jobidparts: ", join(",",@jobidparts),"\n");
      # the first item is the job step id
      # if it is JOBID.0, then it is the allocation ID and we don't want that
      # if it's not 0, then assume it's the one we're looking for
      if jobidparts[1]!='0':
        # we weren't given a pid to check against, assume no match
        if len(argv)==0:
          currjobid = int(fields[0])
          break
        psoutput = runproc(argv=argv,getstdout=True)[0].strip().split(' ')
        if psoutput[0] == str(pid):
          currjobid=int(fields[0])
          break
    return currjobid

  def scr_kill_jobstep(self,jobid=-1):
    if jobid==-1:
      print('You must specify the job step id to kill.')
      return 1
    return runproc(argv=['scancel',str(jobid)])[1]

  def get_scr_end_time(self):
    if self.conf['jobid'] is None:
      return None
    curtime = int(time())
    bjobs, rc = runproc(argv=['bjobs','-o','time_left'],getstdout=True)
    if rc!=0:
      return None
    lines = bjobs.split('\n')
    for line in lines:
      line=line.strip()
      if len(line)==0:
        continue
      if line.startswith('-'):
        # the following is printed if there is no limit
        #   bjobs -o 'time_left'
        #   TIME_LEFT
        #   -
        # look for the "-", in this case,
        # return -1 to indicate there is no limit
        return -1
      pieces = re.split(r'(^\s*)(\d+):(\d+)\s+',line)
      # the following is printed if there is a limit
      #   bjobs -o 'time_left'
      #   TIME_LEFT
      #   0:12 L
      # look for a line like "0:12 L",
      # avoid matching the "L" since other characters can show up there
      if len(pieces)<3:
        continue
      hours = int(pieces[2])
      mins = int(pieces[3])
      secs = curtime + ((hours * 60) + mins) * 60
      return secs
    # had a problem executing bjobs command
    return 0

  # return a hash to define all unavailable (down or excluded) nodes and reason
  def list_down_nodes_with_reason(self,nodes=[],scr_env=None,free=False):
    unavailable = SCR_List_Down_Nodes.list_resmgr_down_nodes(nodes=nodes,resmgr_nodes=self.get_downnodes())
    nextunavail = SCR_List_Down_Nodes.list_pdsh_fail_echo(nodes=nodes)
    unavailable.update(nextunavail)
    if scr_env is not None:
      nextunavail = SCR_List_Down_Nodes.list_param_excluded_nodes(nodes=nodes,param=scr_env.param)
      unavailable.update(nextunavail)
    return unavailable

