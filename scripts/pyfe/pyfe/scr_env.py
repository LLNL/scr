#! /usr/env python

import os, sys, subprocess
import scr_const, scr_hostlist

# SCR_Env class holds the configuration

# def set_prefix(self,prefix):
#   the prefix should be explicitly set (?)

# def set_downnodes(self):
#   set down node list, requires node list to already be set

# def get_runnode_count(self):
#   returns the number of nodes used in the last run
#     could set a member value instead of returning a number

'''
SCR_RESOURCE_MANAGER =
SLURM ; APRUN (cray_xt) ; PMIX ; LSF
'''

class SCR_Env:
  # init initializes vars from the environment
  def __init__(self,env=None):
    if env is None:
      env = scr_const.SCR_RESOURCE_MANAGER
    self.conf = {}
    self.conf['env'] = env
    self.conf['nodes_file'] = scr_const.X_BINDIR+'/scr_nodes_file'
    self.conf['user'] = os.environ.get('USER')
    self.conf['jobid'] = self.getjobid()
    self.conf['nodes'] = self.get_job_nodes()

  # get job id, setting environment flag here
  def getjobid(self):
    val=None
    if self.conf['env'] == 'SLURM':
      val = os.environ.get('SLURM_JOBID')
    elif self.conf['env'] == 'APRUN':
      val = os.environ.get('PBS_JOBID')
    elif self.conf['env'] == 'LSF':
      val = os.environ.get(LSB_JOBID)
    elif self.conf['env'] == 'PMIX':
      # CALL SCR_ENV_HELPER FOR PMIX
      pass
    if val is not None:
      return val
    # failed to read jobid from environment,
    # assume user is running in test mode
    return 'defjobid'

  # get node list
  def get_job_nodes(self):
    if self.conf['env'] == 'SLURM':
      return os.environ.get('SLURM_NODELIST')
    elif self.conf['env'] == 'LSF':
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
      if val is not None:
        # perl code called scr_hostlist.compress
        # that method takes a list though, not a string
        return val
    elif self.conf['env'] == 'APRUN':
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
    elif self.conf['env'] == 'PMIX':
      val = os.environ.get('PMIX_NODELIST')
      if val is not None:
        node_list = val.split(',')
        nodeset = scr_hostlist.compress(node_list)
        return nodeset
    return None

  def get_downnodes(self):
    if self.conf['env'] == 'SLURM':
      val = os.environ.get('SLURM_NODELIST')
      if val is not None:
        argv = ['sinfo','-ho','%N','-t','down','-n',val]
        runproc = subprocess.Popen(argv,bufsize=1,stdin=None,stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell = True, universal_newlines = False)
        down = runproc.communicate()[0]
        if runproc.returncode == 0:
          return down.rstrip()
    elif self.conf['env'] == 'LSF':
      val = os.environ.get('LSB_HOSTS')
      if val is not None:
        # TODO : any way to get list of down nodes in LSF?
        pass
    elif self.conf['env'] == 'APRUN':
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
    elif self.conf['env'] == 'PMIX':
      # if the resource manager knows any nodes to be down out of the job's
      # nodeset, print this list in 'atlas[30-33,35,45-53]' form
      # if there are none, print nothing, not even a newline
      # CALL SCR_ENV_HELPER FOR PMIX - THIS IS A TODO AS PMIX DOESN'T SUPPORT IT YET
      #if (0) {
      #  my $nodeset = ""; #get nodeset with pmixhelper
      pass
    return None

  # set the prefix
  def set_prefix(self,prefix):
    self.conf['prefix'] = prefix

  # set down node list, requires node list to already be set
  def set_downnodes(self):
    # TODO: any way to get list of down nodes in LSF?
    if self.conf['env'] == 'LSF':
      return
    if self.conf['nodes'] is None:
      return
    argv = ['sinfo','-ho','%N','-t','down','-n',','.join(self.conf['nodes'])]
    runproc = subprocess.Popen(args=argv, bufsize=1, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
    out, err = runproc.communicate()
    if runproc.returncode!=0:
      #print('0')
      print(err)
      sys.exit(1)
    self.conf['down'] = out # parse out

  # list the number of nodes used in the last run
  def get_runnode_count(self):
    if self.conf['env'] == 'SLURM' or self.conf['env'] == 'LSF' or self.conf['env'] == 'PMIX':
      argv = [self.conf['nodes_file'],'--dir',self.conf['prefix']]
      runproc = subprocess.Popen(args=argv, bufsize=1, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
      out = runproc.communicate()[0]
      if runproc.returncode==0:
        return int(out)
    elif self.conf['env'] == 'APRUN':
      argv = ['aprun','-n','1',self.conf['nodes_file'],'--dir',self.conf['prefix']]
      runproc = subprocess.Popen(args=argv, bufsize=1, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
      out = runproc.communicate()[0]
      if runproc.returncode == 0:
        return int(out)
    return 0 # print(err)

if __name__ == '__main__':
  scr_env = SCR_Env('SLURM')
  scr_env.set_downnodes()
  for key in scr_env.conf:
    if scr_env.conf[key] == None:
      print('scr_env.conf['+key+'] = None')
    else:
      print('scr_env.conf['+key+'] = \''+scr_env.conf[key]+'\'')

