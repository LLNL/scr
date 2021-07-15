#! /usr/bin/env python3

# lsf.py
# LSF is a subclass of ResourceManager

import os, re
from time import time
from pyfe import scr_const
from pyfe.scr_common import runproc, pipeproc
from pyfe.resmgr import nodetests, ResourceManager

class LSF(ResourceManager):
  # init initializes vars from the environment
  def __init__(self):
    super(LSF, self).__init__(resmgr='LSF')

  # get job id, setting environment flag here
  def getjobid(self):
    if self.conf['jobid'] is not None:
      return self.conf['jobid']
    return os.environ.get('LSB_JOBID')

  def get_jobstep_id(self,user='',pid=-1):
    if user=='' or self.conf['jobid'] is None:
      return -1
    cmd = ['squeue','-h','-s','-u',user,'-j',self.conf['jobid'],'-S','\"-i\"']
    output = runproc(argv=cmd,getstdout=True)[0].split('\n')
    currjobid=-1
    for line in output:
      fields = re.split('\s+',line)
      jobidparts = fields[0].split('.')
      #print "@jobidparts\n";
      # the first item is the job step id
      # if it is JOBID.0, then it is the allocation ID and we don't want that
      # if it's not 0, then assume it's the one we're looking for
      if jobidparts[1]!='0':
        checkPIDcmd = ['ps','h','-p',str(pid)]
        psOutput = runproc(argv=checkPIDcmd,getstdout=True)[0]
        if pdOutput is not None:
          pdOutput = re.split('\s+',pdOutput.strip())
          if pdOutput[0] == str(pid):
            currjobid = str(fields[0])
            break
    return currjobid

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
        hostlist = self.compress_hosts(lines)
        return hostlist
      # failed to read file
      except:
        pass
    val = os.environ.get('LSB_HOSTS')
    if val is not None:
      val = val.split(' ')
      val = self.compress_hosts(val)
    # or, with jobid: squeue -j <jobid> -ho %N
    return val

  def get_downnodes(self):
    val = os.environ.get('LSB_HOSTS')
    if val is not None:
      # TODO : any way to get list of down nodes in LSF?
      pass
    return None

  def scr_kill_jobstep(self,jobid=-1):
    if jobid==-1:
      print('You must specify the job step id to kill.')
      return 1
    return runproc(argv=['bkill','-s','KILL',str(jobid)])[1]

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
  def list_down_nodes_with_reason(self,nodes=[], scr_env=None, free=False, cntldir_string=None, cachedir_string=None):
    unavailable = nodetests.list_resmgr_down_nodes(nodes=nodes, resmgr_nodes=self.expand_hosts(self.get_downnodes()))
    nextunavail = nodetests.list_pdsh_fail_echo(nodes=nodes, nodes_string=self.compress_hosts(nodes), resmgr=self)
    unavailable.update(nextunavail)
    if scr_env is not None and scr_env.param is not None:
      exclude_nodes = self.expand_hosts(scr_env.param.get('SCR_EXCLUDE_NODES'))
      nextunavail = nodetests.list_param_excluded_nodes(nodes=self.expand_hosts(nodes), exclude_nodes=exclude_nodes)
      unavailable.update(nextunavail)
    return unavailable

  # perform a generic pdsh / clustershell command
  # returns [ [ stdout, stderr ] , returncode ]
  def parallel_exec(self, argv=[], runnodes='', use_dshbak=True):
    if len(argv)==0:
      return [ [ '', '' ], 0 ]
    if self.conf['ClusterShell'] == True:
      return self.clustershell_exec(argv=argv, runnodes=runnodes, use_dshbak=use_dshbak)
    pdshcmd = [scr_const.PDSH_EXE, '-Rexec', '-f', '256', '-S', '-w', runnodes]
    pdshcmd.extend(argv)
    if use_dshbak:
      argv = [ pdshcmd, [scr_const.DSHBAK_EXE, '-c'] ]
      return pipeproc(argvs=argv,getstdout=True,getstderr=True)
    return runproc(argv=pdshcmd,getstdout=True,getstderr=True)

  # perform the scavenge files operation for scr_scavenge
  # uses either pdsh or clustershell
  # returns a list -> [ 'stdout', 'stderr' ]
  def scavenge_files(self, prog='', upnodes='', downnodes='', cntldir='', dataset_id='', prefixdir='', buf_size='', crc_flag=''):
    upnodes, downnodes_spaced = self.get_scavenge_nodelists(upnodes=upnodes, downnodes=downnodes)
    argv = [prog, '--cntldir', cntldir, '--id', dataset_id, '--prefix', prefixdir, '--buf', buf_size, crc_flag, downnodes_spaced]
    output = self.parallel_exec(argv=argv,runnodes=upnodes,use_dshbak=False)[0]
    return output
