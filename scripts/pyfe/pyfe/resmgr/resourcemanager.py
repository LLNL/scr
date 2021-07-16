#! /usr/bin/env python3

# resourcemanager
# ResourceManager is the super class of the specific resource manager classes
# the ResourceManager itself is missing its environment type (unknown)
# common methods shared between subclasses can be used from here
# (all environments currently also call the super init)

import os
from pyfe import scr_const, scr_hostlist
from pyfe.scr_common import scr_prefix, runproc

class ResourceManager(object):
  def __init__(self,resmgr='unknown'):
    self.conf = {}
    self.conf['prefix'] = scr_prefix()
    self.conf['resmgr'] = resmgr
    self.conf['use_watchdog'] = False
    self.conf['nodes'] = self.get_job_nodes()
    self.conf['ClusterShell'] = False
    if scr_const.USE_CLUSTERSHELL != '0':
      try:
        import ClusterShell.Task as MyCSTask
        import ClusterShell.NodeSet as MyCSNodeSet
        self.conf['ClusterShell.Task'] = MyCSTask
        self.conf['ClusterShell.NodeSet'] = MyCSNodeSet
        self.conf['ClusterShell'] = True
      except:
        pass

  # no arg -> usewatchdog will return True or False for whether or not watchdog is enabled
  # boolean arg -> default value is to set self.use_watchdog = argument
  # override this method for a specific resource manager to disable use of scr_watchdog
  def usewatchdog(self,use_scr_watchdog=None):
    if use_scr_watchdog is None:
      return self.conf['use_watchdog']
    self.conf['use_watchdog'] = use_scr_watchdog

  def get_jobstep_id(self,user='',pid=-1):
    return -1

  def getjobid(self):
    # failed to read jobid from environment,
    # assume user is running in test mode
    return None

  # get node list
  def get_job_nodes(self):
    return None

  def get_downnodes(self):
    return None

  def scr_kill_jobstep(self,jobid=-1):
    return 1

  def get_scr_end_time(self):
    return 0

  # Returns a hostlist string given a list of hostnames
  def compress_hosts(self,hostnames=[]):
    if hostnames is None or len(hostnames)==0:
      return ''
    if self.conf['ClusterShell']:
      nodeset = self.conf['ClusterShell.NodeSet'].NodeSet.fromlist(hostnames)
      return str(nodeset)
    return scr_hostlist.compress(hostnames)

  # Returns a list of hostnames given a hostlist string
  def expand_hosts(self,hostnames=''):
    if hostnames is None or hostnames=='':
      return []
    if self.conf['ClusterShell']:
      nodeset = self.conf['ClusterShell.NodeSet'].NodeSet(hostnames)
      nodeset = [node for node in nodeset]
      return nodeset
    return scr_hostlist.expand(hostnames)

  # Given references to two lists, subtract elements in list 2 from list 1 and return remainder
  def diff_hosts(self,set1=[],set2=[]):
    if set1 is None or set1==[]:
      return set2 if set2 is not None else []
    if set2 is None or set2==[]:
      return set1
    if self.conf['ClusterShell']:
      set1 = self.conf['ClusterShell.NodeSet'].NodeSet.fromlist(set1)
      set2 = self.conf['ClusterShell.NodeSet'].NodeSet.fromlist(set2)
      set1.difference_update(set2)
      set1 = [node for node in set1]
      return set1
    return scr_hostlist.diff(set1=set1,set2=set2)

  # return a hash to define all unavailable (down or excluded) nodes and reason
  def list_down_nodes_with_reason(self,nodes=[], scr_env=None, free=False, cntldir_string=None, cachedir_string=None):
    return {}

  #####
  #### Return the output as pdsh / dshbak would have (?)
  # https://clustershell.readthedocs.io/en/latest/api/Task.html
  # clustershell exec can be called from any sub-resource manager
  # the sub-resource manager is responsible for ensuring clustershell is available
  ### TODO: different ssh programs may need different parameters added to remove the 'tput: ' from the output
  def clustershell_exec(self, argv=[], runnodes='', use_dshbak=True):
    task = self.conf['ClusterShell.Task'].task_self()
    # launch the task
    task.run(' '.join(argv), nodes=runnodes)
    ret = [ [ '','' ], 0 ]
    # ensure all of the tasks have completed
    self.conf['ClusterShell.Task'].task_wait()
    # iterate through the task.iter_retcodes() to get (return code, [nodes])
    # to get msg objects, output must be retrieved by individual node using task.node_buffer or .key_error
    # retrieved outputs are bytes, convert with .decode('utf-8')
    if use_dshbak:
      # all outputs in each group are the same
      for rc, keys in task.iter_retcodes():
        if rc!=0:
          ret[1] = 1
        # groups may have multiple nodes with identical output, use output of the first node
        output = task.node_buffer(keys[0]).decode('utf-8')
        if len(output)!=0:
          ret[0][0]+='---\n'
          ret[0][0]+=','.join(keys)+'\n'
          ret[0][0]+='---\n'
          lines = output.split('\n')
          for line in lines:
            if line!='' and line!='tput: No value for $TERM and no -T specified':
              ret[0][0]+=line+'\n'
        output = task.key_error(keys[0]).decode('utf-8')
        if len(output)!=0:
          ret[0][1]+='---\n'
          ret[0][1]+=','.join(keys)+'\n'
          ret[0][1]+='---\n'
          lines = output.split('\n')
          for line in lines:
            if line!='' and line!='tput: No value for $TERM and no -T specified':
              ret[0][1]+=line+'\n'
    else:
      for rc, keys in task.iter_retcodes():
        if rc!=0:
          ret[1] = 1
        for host in keys:
          output = task.node_buffer(host).decode('utf-8')
          for line in output.split('\n'):
            if line!='' and line!='tput: No value for $TERM and no -T specified':
              ret[0][0]+=host+': '+line+'\n'
          output = task.key_error(host).decode('utf-8')
          for line in output.split('\n'):
            if line!='' and line!='tput: No value for $TERM and no -T specified':
              ret[0][1]+=host+': '+line+'\n'
    return ret

  # perform a generic pdsh / clustershell command
  # returns [ [ stdout, stderr ] , returncode ]
  def parallel_exec(self, argv=[], runnodes='', use_dshbak=True):
    return [ [ '', '' ], 0 ]

  # each scavenge operation needs upnodes and downnodes_spaced
  def get_scavenge_nodelists(self,upnodes='',downnodes=''):
    # get nodesets
    jobnodes = self.get_job_nodes()
    if jobnodes is None:
      ### error handling
      print('scr_scavenge: ERROR: Could not determine nodeset.')
      return '', ''
    jobnodes = self.expand_hosts(jobnodes)
    if downnodes != '':
      downnodes = self.expand_hosts(downnodes)
      upnodes = self.diff_hosts(jobnodes, downnodes)
    elif upnodes != '':
      upnodes = self.expand_hosts(upnodes)
      downnodes = self.diff_hosts(jobnodes, upnodes)
    else:
      upnodes = jobnodes
    ##############################
    # format up and down node sets for scavenge command
    #################
    upnodes = self.compress_hosts(upnodes)
    downnodes_spaced = ' '.join(downnodes)
    return upnodes, downnodes_spaced

  # perform the scavenge files operation for scr_scavenge
  # command format depends on resource manager in use
  # uses either pdsh or clustershell
  # returns a list -> [ 'stdout', 'stderr' ]
  def scavenge_files(self, prog='', upnodes='', downnodes='', cntldir='', dataset_id='', prefixdir='', buf_size='', crc_flag=''):
    return ['','']

if __name__=='__main__':
  resmgr = ResourceManager()
  print(type(resmgr))
