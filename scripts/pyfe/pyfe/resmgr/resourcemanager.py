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
    self.conf['jobid'] = self.getjobid()
    self.conf['nodes'] = self.get_job_nodes()
    self.conf['clustershell'] = None
    if scr_const.USE_CLUSTERSHELL != '0':
      try:
        import ClusterShell
        self.conf['clustershell'] = ClusterShell
      except:
        pass

  # no arg -> usewatchdog will return True or False for whether or not watchdog is enabled
  # boolean arg -> default value is to set self.use_watchdog = argument
  # override this method for a specific resource manager to disable use of scr_watchdog
  def usewatchdog(self,use_scr_watchdog=None):
    if use_scr_watchdog is None:
      return self.conf['use_watchdog']
    self.conf['use_watchdog'] = use_scr_watchdog

  def getjobid(self):
    # failed to read jobid from environment,
    # assume user is running in test mode
    return None

  # get node list
  def get_job_nodes(self):
    return None

  def get_downnodes(self):
    return None

  def get_jobstep_id(self,user='',pid=-1):
    return -1

  def scr_kill_jobstep(self,jobid=-1):
    return 1

  def get_scr_end_time(self):
    return 0

  # return a hash to define all unavailable (down or excluded) nodes and reason
  def list_down_nodes_with_reason(self,nodes=[], scr_env=None, free=False, cntldir_string=None, cachedir_string=None):
    return {}

  #####
  #### Return the output as pdsh / dshbak would have (?)
  # https://clustershell.readthedocs.io/en/latest/api/Task.html
  # clustershell exec can be called from any sub-resource manager
  # the sub-resource manager is responsible for ensuring clustershell is available
  def clustershell_exec(self, argv=[], runnodes='', use_dshbak=True):
    from ClusterShell.Task import task_self, task_wait
    task = task_self() #self.conf['clustershell'].Task.task_self()
    # launch the task
    task.run(' '.join(argv), nodes=runnodes)
    ret = [ [ '','' ], 0 ]
    # ensure the task has completed
    task.wait(task)
    # iterate through the task.iter_retcodes() to get (return code, [nodes])
    # to get msg objects, output must be retrieved by individual node using task.node_buffer or .key_error
    # retrieved outputs are bytes, convert with .decode('utf-8')
    if use_dshbak:
      # all outputs in each group are the same
      for rc, keys in task.iter_retcodes():
        if rc!=0:
          ret[1] = 1
        print('keys = '+str(keys))
        # groups may have multiple nodes with identical output, use output of the first node
        output = task.node_buffer(keys[0]).decode('utf-8')
        if len(output)!=0:
          ret[0][0]+='---\n'
          ret[0][0]+=','.join(keys)+'\n'
          ret[0][0]+='---\n'
          ret[0][0]+=output.rstrip()+'\n'
        output = task.key_error(keys[0]).decode('utf-8')
        if len(output)!=0:
          ret[0][1]+='---\n'
          ret[0][1]+=','.join(keys)+'\n'
          ret[0][1]+='---\n'
          ret[0][1]+=output.rstrip()+'\n'
    else:
      for rc, keys in task.iter_retcodes():
        if rc!=0:
          ret[1] = 1
        for host in keys:
          output = task.node_buffer(host).decode('utf-8')
          if len(output)!=0:
            for line in output.split('\n'):
              ret[0][0]+=host+': '+line+'\n'
          output = task.key_error(host).decode('utf-8')
          if len(output)!=0:
            for line in output.split('\n'):
              ret[0][1]+=host+': '+line+'\n'
    return ret

  # perform a generic pdsh / clustershell command
  # returns [ [ stdout, stderr ] , returncode ]
  def parallel_exec(self, argv=[], runnodes='', use_dshbak=True):
    return [ [ '', '' ], 0 ]

  # perform the scavenge files operation for scr_scavenge
  # command format depends on resource manager in use
  # uses either pdsh or clustershell
  # returns a list -> [ 'stdout', 'stderr' ]
  def scavenge_files(self, prog='', upnodes='', cntldir='', dataset_id='', prefixdir='', buf_size='', crc_flag='', downnodes_spaced=''):
    return ['','']

if __name__=='__main__':
  resmgr = ResourceManager()
  print(type(resmgr))
