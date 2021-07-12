#! /usr/bin/env python3

# slurm.py
# SLURM is a subclass of ResourceManager

import os, re
from pyfe import scr_const, scr_hostlist
from pyfe.scr_common import runproc, pipeproc
from pyfe.resmgr import nodetests, ResourceManager

# AutoResourceManager class holds the configuration

class SLURM(ResourceManager):
  # init initializes vars from the environment
  def __init__(self):
    super(SLURM, self).__init__(resmgr='SLURM')

  # get job id, setting environment flag here
  def getjobid(self):
    val = os.environ.get('SLURM_JOBID')
    return val

  # get node list
  def get_job_nodes(self):
    return os.environ.get('SLURM_NODELIST')

  def get_downnodes(self):
    val = os.environ.get('SLURM_NODELIST')
    if val is not None:
      argv = ['sinfo','-ho','%N','-t','down','-n',val]
      down, returncode = runproc(argv=argv,getstdout=True)
      if returncode == 0:
        down = down.strip()
        self.conf['down'] = down
        return down
    return None

  def get_jobstep_id(self,user='',pid=-1):
    return self.conf['jobid'] if self.conf['jobid'] is not None
    # we previously weren't able to determine the job id
    # get job steps for this user and job, order by decreasing job step
    # so first one should be the one we are looking for
    # -h means print no header, so just the data in this order:
    # STEPID         NAME PARTITION     USER      TIME NODELIST
    argv = ['squeue','-h','-s','-u',user,'-j',str(self.conf['jobid']),'-S','\"-i\"']
    # my $cmd="squeue -h -s -u $user -j $jobid -S \"-i\"";
    output, returncode = runproc(argv=argv,getstdout=True)
    if returncode != 0:
        return -1
    return re.search('\d+',output)[0]

  def scr_kill_jobstep(self,jobid=-1):
    if jobid==-1:
      print('You must specify the job step id to kill.')
      return 1
    return runproc(argv=['scancel',str(jobid)])[1]

  def get_scr_end_time(self):
    if self.conf['jobid'] is None:
      return None
    argv = []
    argv.append(['scontrol','--oneliner','show','job',self.conf['jobid']])
    argv.append(['perl','-n','-e','\'m/EndTime=(\\S*)/ and print $1\''])
    output = pipeproc(argvs=argv,getstdout=True)[0]
    argv = ['date','-d',output.rstrip()]
    output = runproc(argv=argv,getstdout=True)[0].strip()
    if output.isdigit():
      return int(output)
    return 0

  # return a hash to define all unavailable (down or excluded) nodes and reason
  def list_down_nodes_with_reason(self,nodes=[], scr_env=None, free=False, cntldir_string=None, cachedir_string=None):
    unavailable = nodetests.list_resmgr_down_nodes(nodes=nodes,resmgr_nodes=self.get_downnodes())
    nextunavail = nodetests.list_nodes_failed_ping(nodes=nodes)
    unavailable.update(nextunavail)
    if scr_env is not None:
      nextunavail = nodetests.list_param_excluded_nodes(nodes=nodes, param=scr_env.param)
      unavailable.update(nextunavail)
      argv = [ '$pdsh','-Rexec','-f','256','-w','$upnodes','srun','-n','1','-N','1','-w','%h' ]
      #my $output = `$pdsh -Rexec -f 256 -w '$upnodes' srun -n 1 -N 1 -w %h $bindir/scr_check_node $free_flag $cntldir_flag $cachedir_flag | $dshbak -c`;
      nextunavail = nodetests.check_dir_capacity(nodes=nodes, free=free, scr_env=scr_env, scr_check_node_argv=argv,cntldir_string=cntldir_string,cachedir_string=cachedir_string)
      unavailable.update(nextunavail)
    return unavailable

  # perform the scavenge files operation for scr_scavenge
  # uses either pdsh or clustershell
  # returns a list -> [ 'stdout', 'stderr' ]
  def scavenge_files(self, prog='', upnodes='', cntldir='', dataset_id='', prefixdir='', buf_size='', crc_flag='', downnodes_spaced=''):
    argv = [scr_const.PDSH_EXE, '-Rexec', '-f', '256', '-S', '-w', upnodes, 'srun', '-n1', '-N1', '-w', '%h', prog, '--cntldir', cntldir, '--id', dataset_id, '--prefix', prefixdir, '--buf', buf_size, crc_flag, downnodes_spaced]
    output = runproc(argv=argv,getstdout=True,getstderr=True)[0]
    return output
