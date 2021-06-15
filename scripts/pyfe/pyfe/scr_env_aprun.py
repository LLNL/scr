#! /usr/env python

# scr_env_aprun.py
# SCR_Env_APRUN is a subclass of SCR_Env_Base

import os, sys, subprocess
import scr_const, scr_hostlist
from scr_env_base import SCR_Env_Base

class SCR_Env_APRUN(SCR_Env_Base):
  # init initializes vars from the environment
  def __init__(self,env=None):
    super(SCR_Env_APRUN, self).__init(env='APRUN')

  # get job id, setting environment flag here
  def getjobid(self):
    val = os.environ.get('PBS_JOBID')
    if val is not None:
      return val
    # failed to read jobid from environment,
    # assume user is running in test mode
    return 'defjobid'

  # get node list
  def get_job_nodes(self):
    val = os.environ.get('PBS_NUM_NODES')
    if val is not None:
      argv = ['aprun','-n',val,'-N','1','cat','/proc/cray_xt/nid'] # $nidfile
      runproc = subprocess.Popen(argv,bufsize=1,stdin=None,stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell = True, universal_newlines = False)
      out = runproc.communicate()[0]
      nodearray = out.split('\n')
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
        runproc = subprocess.Popen(argv,bufsize=1,stdin=None,stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell = True, universal_newlines = False)
        out = runproc[0].communicate()[0]
        #if runproc.returncode==0:
        resarray = out.split('\n')
        answerarray = resarray[1].split(' ')
        answer = answerarray[4]
        if 'down' in answer:
          downnodes.append(node)
      if len(downnodes)>0:
        return scr_hostlist.compress(downnodes)
    return None

  # set down node list, requires node list to already be set
  def set_downnodes(self):
    if self.conf['nodes'] is None:
      return
    self.conf['down'] = '' # TODO # parse out

  # list the number of nodes used in the last run
  def get_runnode_count(self):
    argv = ['aprun','-n','1',self.conf['nodes_file'],'--dir',self.conf['prefix']]
    runproc = subprocess.Popen(args=argv, bufsize=1, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
    out = runproc.communicate()[0]
    if runproc.returncode == 0:
      return int(out)
    return 0 # print(err)

