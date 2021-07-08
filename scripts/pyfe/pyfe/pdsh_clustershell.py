#! /usr/bin/env python3

import scr_const
from inspect import ismodule

class MyHandler(EventHandler):
  def ev_read(self,worker,node,sname,msg):
    print(str(node)+': '+str(msg))

  def ev_hup(self,worker,node,rc):
    if rc!=0:
      print(str(node)+': returned with error code '+str(rc))

class PDSH_Clustershell_Exec:
  def __init__(self):
    self.parallelshell = {}
    self.clustershell = None
    if scr_const.USE_CLUSTERSHELL!='0':
      try:
        import ClusterShell
        self.clustershell = ClusterShell
        self.parallelshell['clustershell'] = ClusterShell
      except:
        # do test runtime here, exec $(which pdsh)
        self.parallelshell['pdsh'] = scr_const.PDSH_EXE

  def cmdexec(self,argv,nodelist):
    usingdict = False
    if usingdict:
      if len(self.parallelshell)==0:
        return # error
      pshell = list(self.parallelshell.items())[0][1]
      if type(pshell) is str:
        pass # use pdsh
      else:
        pass # use clustershell
      if ismodule(pshell):
        pass # use clustershell
      else:
        pass # use pdsh
    else:
      if self.clustershell is None:
        pass # exec command with clustershell
      else:
        pass # exec command with pdsh

  # https://clustershell.readthedocs.io/en/latest/guide/examples.html
  # ( also includes a check_nodes.py example script )
  def doclustershell(self):
    # Remote command example (sequential mode)
    task = self.clustershell.Task.task_self()
    task.run('/bin/uname -r', nodes='green[36-39,133]')
    print(task.node_buffer('green37'))
    for buf, nodes in task.iter_buffers():
      print(str(nodes)+' '+str(buf))
    if task.max_retcode() != 0:
      print('An error occured (max rc = '+str(task.max_retcode())+')')
    #result:
    #2.6.32-431.e16.x86_64
    #['green37', 'green38', 'green36', 'green39'] 2.6.32-431.e16.x86_64
    #['green133'] 3.10.0-123.20.1.el7.x86_64
    #Max return code is 0
    ###
    # Remote command example with live output (event-based mode)
    task = self.clustershell.Task.task_self()
    # Submit command, install event handler for this command and run task
    task.run('/bin/uname -a', nodes='fortoy[32-159]', handler=MyHandler())

  ### Using NodeSet with Parallel Python Batch script using SLURM

  # example of a script.py in SLURM
  '''
  #! /bin/bash
  #SBATCH -N 2
  #SBATCH --ntasks-per-node 1
  # run the servers
  srun ~/.local/bin/ppserver.py -w $SLURM_CPUS_PER_TASK -t 300 &
  sleep 10
  # launch the parallel processing
  python -u ./pp_jobs.py
  '''

  # example was import pp (for pp_jobs.py)
  # test function to execute on remote nodes
  def test_func():
    print(os.uname())

  def doslurmpyscript(self):
    # get the nodeset from slurm
    nodelist = self.clustershell.NodeSet.NodeSet(os.environ['SLURM_NODELIST'])
    # start the servers (ncpus=0 will make sure that none is started locally)
    # casting nodelist to tuple/list will correctly expand $SLURM_NODELIST
    job_server = pp.Server(ncpus=0, ppservers=tuple(nodelist))
    # make sure the servers have enough time to start
    time.sleep(5)
    # start the jobs
    job_1 = job_server.submit(test_func,(),(),('os',))
    job_2 = job_server.submit(test_func,(),(),('os',))
    # retrieve the results
    print(job_1)
    print(job_2)
    # cleanup
    job_server.print_stats()
    job_server.destroy()

