#! /usr/bin/env python3

# pbsalps.py
# cray xt
# PBSALPS is a subclass of ResourceManager

import os, re
from pyfe import scr_const
from pyfe.scr_common import runproc, pipeproc
from pyfe.resmgr import nodetests, ResourceManager

class PBSALPS(ResourceManager):
  # init initializes vars from the environment
  def __init__(self,env=None):
    super(PBSALPS, self).__init__(resmgr='PBSALPS')

  # get job id, setting environment flag here
  def getjobid(self):
    if self.conf['jobid'] is not None:
      return self.conf['jobid']
    # val may be None
    return os.environ.get('PBS_JOBID')

  def get_jobstep_id(self,user='',pid=-1):
    output = runproc(argv=['apstat','-avv'],getstdout=True)[0].split('\n')
    nid = None
    try:
      with open('/proc/cray_xt/nid','r') as NIDfile:
        nid = NIDfile.read()[:-1]
    except:
      pass
    if nid is None: # or value not sane
      #### Are we unable to continue ?
      return -1
    currApid=-1
    for line in output:
      line=line.strip()
      if len(line)==0:
        continue
      fields = re.split('\s+',line)
      if fields[0].startswith('Ap'):
        currApid=int(fields[2][:-1])
      elif fields[1].startswith('Originator:'):
         #did we find the apid that corresponds to the pid?
         # also check to see if it was launched from this MOM node in case two
         # happen to have the same pid
        thisnid = int(fields[5][:-1])
        if thisnid == nid and fields[7] == pid:
          break
        currApid=-1
    return currApid

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
          shortnodes = self.compress_hosts(nodearray)
          return shortnodes
    return None

  def get_downnodes(self):
    downnodes = []
    snodes = self.get_job_nodes()
    if snodes is not None:
      snodes = self.expand_hosts(snodes)
      argv = ['xtprocadmin', '-n', ''] # $xtprocadmin
      for node in snodes:
        argv[2] = node
        out, returncode = runproc(argv=argv, getstdout=True)
        #if returncode==0:
        resarray = out.split('\n')
        answerarray = resarray[1].split(' ')
        answer = answerarray[4]
        if 'down' in answer:
          downnodes.append(node)
      if len(downnodes)>0:
        return self.compress_hosts(downnodes)
    return []

  def scr_kill_jobstep(self,jobid=-1):
    if jobid==-1:
      print('You must specify the job step id to kill.')
      return 1
    return runproc(argv=['apkill',str(jobid)])[1]

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
