#! /usr/bin/env python3

# scr_resourcemgr_aprun.py
# cray xt
# SCR_Resourcemgr_APRUN is a subclass of SCR_Resourcemgr_Base

import os, re
from pyfe import scr_const, scr_hostlist
from pyfe.resmgr.scr_resourcemgr_base import SCR_Resourcemgr_Base
from pyfe.scr_common import runproc
from pyfe.scr_list_down_nodes import SCR_List_Down_Nodes

class SCR_Resourcemgr_APRUN(SCR_Resourcemgr_Base):
  # init initializes vars from the environment
  def __init__(self,env=None):
    super(SCR_Resourcemgr_APRUN, self).__init__(resmgr='APRUN')

  # get job id, setting environment flag here
  def getjobid(self):
    val = os.environ.get('PBS_JOBID')
    # val may be None
    return val

  # get node list
  def get_job_nodes(self):
    val = os.environ.get('PBS_NUM_NODES')
    if val is not None:
      argv = ['aprun','-n',val,'-N','1','cat','/proc/cray_xt/nid'] # $nidfile
      out = runproc(argv=argv,getstdout=True)[0]
      nodearray = out.split('\n')
      if len(nodearray)>0:
        if nodearray[-1]=='\n':
          nodearray=nodearray[:-1]
        if len(nodearray)>0:
          if nodearray[-1].startswith('Application'):
            nodearray=nodearray[:-1]
          shortnodes = scr_hostlist.compress(nodearray)
          return shortnodes
    return None

  def get_downnodes(self):
    downnodes = []
    snodes = self.get_job_nodes()
    if snodes is not None:
      snodes = scr_hostlist.expand(snodes)
      argv = ['xtprocadmin', '-n', ''] # $xtprocadmin
      for node in nodes:
        argv[2] = node
        out, returncode = runproc(argv=argv, getstdout=True)
        #if returncode==0:
        resarray = out.split('\n')
        answerarray = resarray[1].split(' ')
        answer = answerarray[4]
        if 'down' in answer:
          downnodes.append(node)
      if len(downnodes)>0:
        return scr_hostlist.compress(downnodes)
    return None

  # list the number of nodes used in the last run
  def get_runnode_count(self):
    argv = ['aprun','-n','1',self.conf['nodes_file'],'--dir',self.conf['prefix']]
    out, returncode = runproc(argv=argv,getstdout=True)
    if runproc.returncode == 0:
      return int(out)
    return 0 # print(err)

  def get_jobstep_id(self,user='',pid=-1):
    output = runproc(argv=['apstat','-avv'],getstdout=True)[0].split('\n')
    # we could use 'head' instead of cat or do a with open ?
    nid = runproc(argv=['cat','/proc/cray_xt/nid'],getstdout=True)[0].strip().split('\n')[0] #just the top line
    currApid=-1
    for line in output:
      line=line.strip()
      if len(line)<1:
        continue
      fields = re.split('\s+',line)
      fields = line.strip().split(' ')
      if fields[0].startswith('Ap'):
        currApid=int(fields[2][:-1])
      elif fields[1].startswith('Originator:'):
         #did we find the apid that corresponds to the pid?
         # also check to see if it was launched from this MOM node in case two
         # happen to have the same pid
        thisnid = fields[5][:-1]
        if thisnid == nid and fields[7] == pid: # pid is used in this one.
          break
        currApid=-1
    return currApid

  def scr_kill_jobstep(self,jobid=-1):
    if jobid==-1:
      print('You must specify the job step id to kill.')
      return 1
    return runproc(argv=['apkill',str(jobid)])[1]

  # return a hash to define all unavailable (down or excluded) nodes and reason
  def list_down_nodes_with_reason(self,nodes=[],scr_env=None,free=False):
    unavailable = SCR_List_Down_Nodes.list_resmgr_down_nodes(nodes=nodes,resmgr_nodes=self.get_downnodes())
    nextunavail = SCR_List_Down_Nodes.list_nodes_failed_ping(nodes=nodes)
    unavailable.update(nextunavail)
    if scr_env is not None:
      nextunavail = SCR_List_Down_Nodes.list_param_excluded_nodes(nodes=nodes,param=scr_env.param)
      unavailable.update(nextunavail)
      argv = [ '$pdsh','-Rexec','-f','256','-w','$upnodes','aprun','-n','1','-L','%h' ]
      #my $output = `$pdsh -Rexec -f 256 -w '$upnodes' aprun -n 1 -L %h $bindir/scr_check_node $free_flag $cntldir_flag $cachedir_flag | $dshbak -c`;
      nextunavail = SCR_List_Down_Nodes.check_dir_capacity(nodes=nodes, free=free, scr_env=scr_env, scr_check_node_argv=argv)
      unavailable.update(nextunavail)
    return unavailable

