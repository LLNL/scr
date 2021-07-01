#! /usr/bin/env python3

# scr_resourcemgr_slurm.py
# SCR_Resourcemgr_SLURM is a subclass of SCR_Resourcemgr_Base

import os, re
from pyfe import scr_const, scr_hostlist
from pyfe.resmgr import ResourceManager
from pyfe.scr_common import runproc
from pyfe.scr_list_down_nodes import SCR_List_Down_Nodes

# SCR_Resourcemgr class holds the configuration

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

  # list the number of nodes used in the last run
  def get_runnode_count(self):
    argv = [self.conf['nodes_file'],'--dir',self.conf['prefix']]
    out, returncode = runproc(argv=argv,getstdout=True)
    if returncode==0:
      return int(out)
    return 0 # print(err)

  def get_jobstep_id(self,user='',pid=-1):
    # we previously weren't able to determine the job id
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
      if jobidparts[1]!='0' and jobidparts[1]!='batch':
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
    argv = []
    argv.append(['scontrol','--oneliner','show','job',jobid])
    argv.append(['perl','-n','-e','\'m/EndTime=(\\S*)/ and print $1\''])
    output = pipeproc(argvs=argv,getstdout=True)[0]
    argv = ['date','-d',output.rstrip()]
    output = runproc(argv=argv,getstdout=True)[0].strip()
    if output.isdigit():
      return int(output)
    return 0

  # return a hash to define all unavailable (down or excluded) nodes and reason
  def list_down_nodes_with_reason(self,nodes=[],scr_env=None,free=False):
    unavailable = SCR_List_Down_Nodes.list_resmgr_down_nodes(nodes=nodes,resmgr_nodes=self.get_downnodes())
    nextunavail = SCR_List_Down_Nodes.list_nodes_failed_ping(nodes=nodes)
    unavailable.update(nextunavail)
    if scr_env is not None:
      nextunavail = SCR_List_Down_Nodes.list_param_excluded_nodes(nodes=nodes,param=scr_env.param)
      unavailable.update(nextunavail)
      argv = [ '$pdsh','-Rexec','-f','256','-w','$upnodes','srun','-n','1','-N','1','-w','%h' ]
      #my $output = `$pdsh -Rexec -f 256 -w '$upnodes' srun -n 1 -N 1 -w %h $bindir/scr_check_node $free_flag $cntldir_flag $cachedir_flag | $dshbak -c`;
      nextunavail = SCR_List_Down_Nodes.check_dir_capacity(nodes=nodes, free=free, scr_env=scr_env, scr_check_node_argv=argv)
      unavailable.update(nextunavail)
    return unavailable

