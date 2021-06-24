#! /usr/bin/env python3

# scr_resourcemgr_lsf.py
# SCR_Resourcemgr_LSF is a subclass of SCR_Resourcemgr_Base

import os, re
from pyfe import scr_const, scr_hostlist
from pyfe.resmgr.scr_resourcemgr_base import SCR_Resourcemgr_Base
from pyfe.scr_common import runproc

class SCR_Resourcemgr_LSF(SCR_Resourcemgr_Base):
  # init initializes vars from the environment
  def __init__(self):
    super(SCR_Resourcemgr_LSF, self).__init__(resmgr='LSF')

  # get job id, setting environment flag here
  def getjobid(self):
    val = os.environ.get('LSB_JOBID')
    if val is not None:
      return val
    # failed to read jobid from environment,
    # assume user is running in test mode
    return 'defjobid'

  # get node list
  def get_job_nodes(self):
    val = os.environ.get('LSB_DJOB_HOSTFILE')
    if val is not None:
      with open(val,'r') as hostfile:
        # make a list from the set -> make a set from the list -> file.readlines().rstrip('\n')
        # get a list of lines without newlines and skip the first line
        lines = [line.rstrip() for line in hostfile.readlines()][1:]
        # get a set of unique hostnames, convert list to set and back
        hosts_unique = list(set(lines))
        hostlist = scr_hostlist.compress(hosts_unique)
        return hostlist
    val = os.environ.get('LSB_HOSTS')
    # perl code called scr_hostlist.compress
    # that method takes a list though, not a string
    return val

  def get_downnodes(self):
    val = os.environ.get('LSB_HOSTS')
    if val is not None:
      # TODO : any way to get list of down nodes in LSF?
      pass
    return None

  # list the number of nodes used in the last run
  def get_runnode_count(self):
    argv = [self.conf['nodes_file'],'--dir',self.conf['prefix']]
    out, returncode = runproc(argv=argv,getstdout=True)
    if returncode==0:
      return int(out.rstrip())
    return 0 # print(err)

  def get_jobstep_id(user='',jobid='',pid=-2):
    # the slurm get_jobstep_id didn't use the pid parameter
    # get job steps for this user and job, order by decreasing job step
    # so first one should be the one we are looking for
    # -h means print no header, so just the data in this order:
    # STEPID         NAME PARTITION     USER      TIME NODELIST
    argv = ['squeue','-h','-s','-u',user,'-j',jobid,'-S','\"-i\"']
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
