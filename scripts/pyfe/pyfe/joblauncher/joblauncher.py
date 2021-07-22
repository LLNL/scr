#! /usr/bin/env python3

# joblauncher.py
# JobLauncher is the super class for the job launcher family

from pyfe import scr_const

class JobLauncher(object):
  def __init__(self,launcher=''):
    self.launcher = launcher
    self.hostfile = ''
    self.resmgr = None
    self.clustershell_task = False
    if scr_const.USE_CLUSTERSHELL != '0':
      try:
        import ClusterShell.Task as MyCSTask
        self.clustershell_task = MyCSTask
      except:
        pass

  # if a job launcher would like to perform any operations before scr_prerun
  def prepareforprerun(self):
    pass

  # some launchers use only up or down nodes.
  # the run_cmd or restart_cmd will be appended to launcher_args already
  # returns the process and PID of the launched process
  # as returned by runproc(argv=argv, wait=False)
  def launchruncmd(self,up_nodes='',down_nodes='',launcher_args=[]):
    # an empty argv will just immediately return
    # could return something like: ['echo','unknown launcher']
    return None, -1

  #####
  #### Return the output as pdsh / dshbak would have (?)
  # https://clustershell.readthedocs.io/en/latest/api/Task.html
  # clustershell exec can be called from any sub-resource manager
  # the sub-resource manager is responsible for ensuring clustershell is available
  ### TODO: different ssh programs may need different parameters added to remove the 'tput: ' from the output
  def clustershell_exec(self, argv=[], runnodes='', use_dshbak=True):
    task = self.clustershell_task.task_self()
    # launch the task
    task.run(' '.join(argv), nodes=runnodes)
    ret = [ [ '','' ], 0 ]
    # ensure all of the tasks have completed
    self.clustershell_task.task_wait()
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

  # perform the scavenge files operation for scr_scavenge
  # command format depends on resource manager in use
  # uses either pdsh or clustershell
  # returns a list -> [ 'stdout', 'stderr' ]
  def scavenge_files(self, prog='', upnodes='', downnodes='', cntldir='', dataset_id='', prefixdir='', buf_size='', crc_flag=''):
    return ['','']
