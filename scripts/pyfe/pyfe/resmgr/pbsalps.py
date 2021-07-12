#! /usr/bin/env python3

# pbsalps.py
# cray xt
# PBSALPS is a subclass of ResourceManager

import os, re
from pyfe import scr_const, scr_hostlist
from pyfe.scr_common import runproc, pipeproc
from pyfe.resmgr import nodetests, ResourceManager

class PBSALPS(ResourceManager):
  # init initializes vars from the environment
  def __init__(self,env=None):
    super(PBSALPS, self).__init__(resmgr='PBSALPS')

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

  def get_jobstep_id(self,user='',pid=-1):
    return self.conf['jobid'] if self.conf['jobid'] is not None
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
  def list_down_nodes_with_reason(self,nodes=[], scr_env=None, free=False, cntldir_string=None, cachedir_string=None):
    unavailable = nodetests.list_resmgr_down_nodes(nodes=nodes,resmgr_nodes=self.get_downnodes())
    nextunavail = nodetests.list_nodes_failed_ping(nodes=nodes)
    unavailable.update(nextunavail)
    if scr_env is not None:
      nextunavail = nodetests.list_param_excluded_nodes(nodes=nodes,param=scr_env.param)
      unavailable.update(nextunavail)
      # assert scr_env.resmgr == self
      nextunavail = nodetests.check_dir_capacity(nodes=nodes, free=free, scr_env=scr_env, cntldir_string=cntldir_string, cachedir_string=cachedir_string)
      unavailable.update(nextunavail)
    return unavailable

  # perform a generic pdsh / clustershell command
  # returns [ [ stdout, stderr ] , returncode ]
  def parallel_exec(self, argv=[], runnodes='', use_dshbak=True):
    if len(argv==0):
      return [ [ '', '' ], 0 ]
    pdshcmd = [scr_const.PDSH_EXE, '-Rexec', '-f', '256', '-S', '-w', runnodes]
    pdshcmd.extend(argv)
    if use_dshbak:
      argv = [ pdshcmd, [scr_const.DSHBAK_EXE, '-c'] ]
      return pipeproc(argvs=argv,getstdout=True,getstderr=True)
    return runproc(argv=pdshcmd,getstdout=True,getstderr=True)

  # perform the scavenge files operation for scr_scavenge
  # uses either pdsh or clustershell
  # returns a list -> [ 'stdout', 'stderr' ]
  def scavenge_files(self, prog='', upnodes='', cntldir='', dataset_id='', prefixdir='', buf_size='', crc_flag='', downnodes_spaced=''):
    argv = ['aprun', '-n', '1', 'L', '%h', prog, '--cntldir', cntldir, '--id', dataset_id, '--prefix', prefixdir, '--buf', buf_size, crc_flag]
    container_flag = scr_env.param.get('SCR_USE_CONTAINERS')
    if container_flag is None or container_flag!='0':
      argv.append('--containers')
    argv.append(downnodes_spaced)
    output = self.parallel_exec(argv=argv,runnodes=upnodes,use_dshbak=False)[0]
    return output
