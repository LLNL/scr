#! /usr/bin/env python3

# scr_run.py

# the general launcher for the scripts
# if called directly the launcher to use (srun/jsrun/mpirun) should be specified as an argument
# scr_{srun,jsrun,mpirun} scripts call this script with the launcher specified

import os, sys

if 'pyfe' not in sys.path:
  sys.path.insert(0,'/'.join(os.path.realpath(__file__).split('/')[:-2]))
  import pyfe

from datetime import datetime
import signal
import multiprocessing as mp
from time import time, sleep
from pyfe import scr_const, scr_common
from pyfe.postrun import postrun
from pyfe.list_dir import list_dir
from pyfe.list_down_nodes import list_down_nodes
from pyfe.scr_common import tracefunction, runproc, scr_prefix
from pyfe.scr_prerun import scr_prerun
from pyfe.scr_get_jobstep_id import scr_get_jobstep_id
from pyfe.scr_watchdog import scr_watchdog
from pyfe.scr_environment import SCR_Env
from pyfe.joblauncher import AutoJobLauncher
from pyfe.resmgr import AutoResourceManager
from pyfe.scr_param import SCR_Param
from pyfe.scr_glob_hosts import scr_glob_hosts

# determine how many nodes are needed
def nodes_needed(scr_env, nodelist):
  # if SCR_MIN_NODES is set, use that
  num_needed = os.environ.get('SCR_MIN_NODES')
  if num_needed is None or int(num_needed) <= 0:
    # otherwise, use value in nodes file if one exists
    # num_needed_env='$bindir/scr_env --prefix $prefix --runnodes'
    num_needed = scr_env.get_runnode_count()
    if num_needed <= 0:
      # otherwise, assume we need all nodes in the allocation
      num_needed = scr_glob_hosts(count=True, hosts=nodelist, resmgr=scr_env.resmgr)
      if num_needed is None:
        # failed all methods to estimate the minimum number of nodes
        return 0
  return int(num_needed)

# return number of nodes left in allocation after excluding down nodes
def nodes_remaining(resmgr, nodelist, down_nodes):
  # num_left='$bindir/scr_glob_hosts --count --minus $SCR_NODELIST:$down_nodes'
  num_left = scr_glob_hosts(count=True, minus = nodelist + ':' + down_nodes, resmgr=resmgr)
  if num_left is None:
    return 0
  return int(num_left)

# is there a halt condition instructing us to stop?
def should_halt(bindir, prefix):
  #$bindir/scr_retries_halt --dir $prefix;
  argv = [bindir + '/scr_retries_halt', '--dir', prefix]
  returncode = runproc(argv=argv)[1]
  return (returncode == 0)

def scr_run(launcher='',launcher_args=[],run_cmd='',restart_cmd='',restart_args=[]):
  if launcher=='':
    launcher=scr_const.SCR_LAUNCHER
    if launcher == '':
      print('Launcher must be specified')
      return 1
  prog='scr_'+launcher

  libdir=scr_const.X_LIBDIR
  bindir=scr_const.X_BINDIR

  val = os.environ.get('SCR_ENABLE')
  if val=='0':
    launcher = [launcher]
    launcher.extend(launcher_args)
    returncode = runproc(argv=launcher)[1]
    sys.exit(returncode)

  # turn on verbosity
  val = os.environ.get('SCR_DEBUG')
  verbose = False
  if val is not None and int(val)>0:
    verbose=True

  # turn on python function tracing
  if scr_const.PYFE_TRACE_FUNC=='1' or os.environ.get('PYFE_TRACE_FUNC')=='1':
    sys.settrace(tracefunction)

  # make a record of start time
  timestamp=datetime.now()
  start_secs = int(time())
  print(prog+': Started: '+str(timestamp))

  # TODO: if not in job allocation, bail out

  param = SCR_Param()
  scr_env = SCR_Env() # env contains general environment infos independent of resmgr/launcher
  resourcemgr = AutoResourceManager() # resource manager (SLURM/LSF/ ...) set by argument or compile constant
  launcher = AutoJobLauncher(launcher) # launcher contains attributes unique to launcher (srun/jsrun/ ...)
  # give scr_env a pointer to the objects for calling other methods
  scr_env.param = param
  scr_env.resmgr = resourcemgr
  scr_env.launcher = launcher
  launcher.resmgr = resourcemgr
  # this may be used by a launcher to store a list of hosts
  launcher.hostfile = scr_env.get_prefix()+'/.scr/hostfile'
  # jobid will come from resource manager.
  jobid = resourcemgr.getjobid()

  # TODO: check that we have a valid jobid and bail if not
  # pmix always returns None
  # others return None when it can't be found
  # previously they returned 'defjobid' with the comment to assume testing
  if jobid is None: #### pmix always returns none.
    jobid = 'defjobid'
    #print(prog+': ERROR: Could not determine jobid.')
    #sys.exit(1)

  # get the nodeset of this job
  nodelist = scr_env.get_scr_nodelist()
  if nodelist is None:
    nodelist = scr_env.resmgr.get_job_nodes()
    if nodelist is None:
      print(prog+': ERROR: Could not identify nodeset')
      sys.exit(1)

  # get prefix directory
  prefix=scr_env.get_prefix()

  val=os.environ.get('SCR_WATCHDOG')
  if val is None or val!='1':
    resourcemgr.usewatchdog(False)
  else:
    resourcemgr.usewatchdog(True)

  # get the control directory
  cntldir = list_dir(user=scr_env.get_user(), jobid=resourcemgr.getjobid(), runcmd='control', scr_env=scr_env, bindir=bindir)
  if cntldir == 1:
    print(prog+': ERROR: Could not determine control directory')
    sys.exit(1)

  # run a NOP with srun, other launchers could do any preamble work here
  launcher.prepareforprerun()

  # make a record of time prerun is started
  timestamp=datetime.now()
  print(prog+': prerun: '+str(timestamp))

  # test runtime, ensure filepath exists,
  if scr_prerun(prefix=prefix)!=0:
    print(prog+': ERROR: Command failed: scr_prerun -p '+prefix)
    sys.exit(1)

  endtime = resourcemgr.get_scr_end_time()
  if endtime == 0:
    # no function to get end time for pmix / aprun (crayxt)
    if verbose==True:
      print(prog+': WARNING: Unable to get end time.')
  elif endtime == -1: # no end time / limit
    pass
  os.environ['SCR_END_TIME'] = str(endtime)

  # enter the run loop
  down_nodes=''
  attempts=0
  runs = os.environ.get('SCR_RUNS')
  if runs is None:
    runs = os.environ.get('SCR_RETRIES')
    if runs is None:
      runs=0
    else:
      runs=int(runs)
    runs+=1
  else:
    runs=int(runs)
  # totalruns printed when runs are exhausted
  totalruns = str(runs)

  while True:
    # once we mark a node as bad, leave it as bad (even if it comes back healthy)
    # TODO: This hacks around the problem of accidentally deleting a checkpoint set during distribute
    #       when a relaunch lands on previously down nodes, which are healthy again.
    #       A better way would be to remember the last set used, or to provide a utility to run on *all*
    #       nodes to distribute files (also useful for defragging the machine) -- for now this works.

    keep_down = down_nodes
    # if this is our first run, check that the free space on the drive meets requirement
    # (make sure data from job of previous user was cleaned up ok)
    # otherwise, we'll just check the total capacity
    free_flag=False
    if attempts == 0:
      free_flag=True

    # are there enough nodes to continue?
    down_nodes = list_down_nodes(free=free_flag,nodeset_down=down_nodes,scr_env=scr_env)
    # returns 0 for none, 1 for error, or a string
    # could handle error here
    if type(down_nodes) is int:
      down_nodes = ''
    if down_nodes!='':
      # print the reason for the down nodes, and log them
      list_down_nodes(reason=True, free=free_flag, nodeset_down=down_nodes, log_nodes=True, runtime_secs='0', scr_env=scr_env)

      # if this is the first run, we hit down nodes right off the bat, make a record of them
      if attempts==0:
        start_secs=int(time())
        print('SCR: Failed node detected: JOBID='+jobid+' ATTEMPT=0 TIME='+str(start_secs)+' NNODES=-1 RUNTIME=0 FAILED='+down_nodes)

      ##### Unless this value may change at some point in an allocation
      ##### This could be moved outside of this loop, this value is not changed within the loop
      ##### *  num_needed  *

      # determine how many nodes are needed
      num_needed = nodes_needed(scr_env, nodelist)
      if num_needed <= 0:
        print(prog + ': ERROR: Unable to determine number of nodes needed')
        break

      # determine number of nodes remaining in allocation
      num_left = nodes_remaining(resourcemgr, nodelist, down_nodes)
      if num_left <= 0:
        print(prog + ': ERROR: Unable to determine number of nodes remaining')
        break

      # check that we have enough nodes after excluding down nodes
      if num_left < num_needed:
        print(prog + ': (Nodes remaining=' + str(num_left) + ') < (Nodes needed=' + str(num_needed) + '), ending run.')
        break

    # make a record of when each run is started
    attempts += 1
    timestamp = datetime.now()
    print(prog + ': RUN ' + str(attempts) + ': ' + str(timestamp))

    launch_cmd=launcher_args.copy()
    launch_cmd.append(run_cmd)
    if restart_cmd!='':
      argv = [launcher]
      argv.extend(launcher_args)
      argv.append(bindir+'/scr_have_restart')
      restart_name = runproc(argv=argv,getstdout=True)[0]
      if restart_name is not None and restart_name!='':
        restart_name=restart_name.strip()
        my_restart_cmd = re.sub('SCR_CKPT_NAME',restart_name,restart_cmd)
        launch_cmd = launcher_args.copy()
        ###### should this be split (' ') ?
        launch_cmd.extend(my_restart_cmd.split(' '))
    # launch the job, make sure we include the script node and exclude down nodes
    start_secs=int(time())

    scr_common.log(bindir=bindir, prefix=prefix, jobid=jobid, event_type='RUN_START', event_note='run='+str(attempts), event_start=str(start_secs))
    # $bindir/scr_log_event -i $jobid -p $prefix -T "RUN_START" -N "run=$attempts" -S $start_secs
    print(prog + ': Launching ' + str(launch_cmd))
    proc, pid = launcher.launchruncmd(up_nodes=nodelist,down_nodes=down_nodes,launcher_args=launch_cmd)
    # $launcher $exclude $launch_cmd
    if resourcemgr.usewatchdog() == False:
      proc.wait(timeout=None)
    else:
      print(prog + ': Entering watchdog method')
      # The watchdog will return when the process finishes or is killed
     i scr_watchdog(prefix=prefix, watched_process=proc, scr_env=scr_env)

    #print('Process has finished or has been terminated.')

    end_secs = int(time())
    run_secs = end_secs - start_secs

    # check for and log any down nodes
    list_down_nodes(reason=True, nodeset_down=keep_down, log_nodes=True, runtime_secs=str(run_secs), scr_env=scr_env)
    # log stats on the latest run attempt
    scr_common.log(bindir=bindir, prefix=prefix, jobid=jobid, event_type='RUN_END', event_note='run='+str(attempts), event_start=str(end_secs), event_secs=str(run_secs))
    #$bindir/scr_log_event -i $jobid -p $prefix -T "RUN_END" -N "run=$attempts" -S $end_secs -L $run_secs

    # any retry attempts left?
    if runs > 1:
      runs -= 1
      if runs == 0:
        print(prog+': '+totalruns+' exhausted, ending run.')
        break

    # is there a halt condition instructing us to stop?
    if should_halt(bindir, prefix):
      print(prog + ': Halt condition detected, ending run.')
      break

    ##### Reduce this sleep time when testing scripts #####
    # give nodes a chance to clean up
    sleep(60)

    # check for halt condition again after sleep
    if should_halt(bindir, prefix):
      print(prog + ': Halt condition detected, ending run.')
      break

  # make a record of time postrun is started
  timestamp = datetime.now()
  print(prog + ': postrun: ' + str(timestamp))

  # scavenge files from cache to parallel file system
  if postrun(prefix_dir=prefix,scr_env=scr_env,verbose=verbose) != 0:
    print(prog+': ERROR: Command failed: scr_postrun -p '+prefix)

  # make a record of end time
  timestamp = datetime.now()
  print(prog + ': Ended: ' + str(timestamp))

def print_usage(launcher=''):
  available_launchers = '[srun/jsrun/mpirun]'
  print('USAGE:')
  # the original parsing in SLURM/scr_run.in combines all [launcher args]
  # (no matter whether they appear in the front or at the end)
  # the usage printing looks like you could define/usr different launcher args
  if launcher=='':
    launcher=available_launchers
    print('scr_run <launcher> ['+launcher+' args] [-rc|--run-cmd=<run_command>] [-rs|--restart-cmd=<restart_command>] ['+launcher+' args]')
    print('<launcher>: The job launcher to use, one of '+launcher)
  else:
    print('scr_'+launcher+' ['+launcher+' args] [-rc|--run-cmd=<run_command>] [-rs|--restart-cmd=<restart_command>] ['+launcher+' args]')
  print('<run_command>: The command to run when no restart file is present')
  print('<restart_command>: The command to run when a restart file is present')
  print('')
  print('The invoked command will be \''+launcher+' ['+launcher+' args] [run_command]\' when no restart file is present')
  print('The invoked command will be \''+launcher+' ['+launcher+' args] [restart_command]\' when a restart file is present')
  print('If the string \"SCR_CKPT_NAME\" appears in the restart command, it will be replaced by the name')
  print('presented to SCR when the most recent checkpoint was written.')
  print('')
  print('If no restart command is specified, the run command will always be used')
  print('If no commands are specified, the '+launcher+' arguments will be passed directly to '+launcher+' in all circumstances')
  print('If no run command is specified, but a restart command is specified,')
  print('then the restart command will be appended to the '+launcher+' arguments when a restart file is present.')

def parseargs(argv,launcher=''):
  starti = 0
  if launcher=='':
    launcher = argv[0]
    starti=1
  launcher_args = []
  run_cmd = ''
  restart_cmd = ''
  restart_args = []
  # position starts at zero to parse args
  position = 0
  for i in range(starti,len(argv)):
    # pos ->       0                           1                                   2                     3
    # ['+launcher+' args] [-rc|--run-cmd=<run_command>] [-rs|--restart-cmd=<restart_command>] ['+launcher+' args]')
    # haven't gotten to run command yet
    if position==0:
      # run command is next (or here if we have an '=')
      if argv[i].startswith('-rc') or argv[i].startswith('--run-cmd'):
        if '=' in argv[i]:
          run_cmd = argv[i][argv[i].find('=')+1:] # take after the equals
        else:
          position=1
      # no run command but got to the restart command
      elif argv[i].startswith('-rs') or argv[i].startswith('--restart-cmd'):
        if '=' in argv[i]:
          restart_cmd = argv[i][argv[i].find('=')+1:] # take after the equals
          position=3 # final args
        else:
          position=2
      # these are launcher args before the run command
      else:
        launcher_args.append(argv[i])
    # position is 1, we are expecting the run command
    elif position==1:
      run_cmd = argv[i]
      position=0
    # expecting the restart command
    elif position==2:
      restart_cmd = argv[i]
      position=3
    # final launcher args
    elif position==3:
      if argv[i].startswith('-rc') or argv[i].startswith('--run-cmd'):
        if '=' in argv[i]:
          run_cmd = argv[i][argv[i].find('=')+1:] # take after the equals
          position=0
        else:
          position = 1
      # these are trailing restart args
      else:
        restart_args.append(argv[i])
  return launcher, launcher_args, run_cmd, restart_cmd, restart_args

# argparse doesn't handle parsing of mixed positionals
# there is a parse_intermixed_args which requires python 3.7 which may work
# I'm just going to skip the argparse
if __name__=='__main__':
  # just printing help, print the help and exit
  if len(sys.argv)<3 or '-h' in sys.argv[1:] or '--help' in sys.argv[1:]:
    print_usage()
  elif not any(arg.startswith('-h') or arg.startswith('--help') or arg.startswith('-rc') or arg.startswith('--run-cmd') or arg.startswith('-rs') or arg.startswith('--restart-cmd') for arg in sys.argv):
    # then we were called with: scr_run launcher [args]
    scr_run(launcher=sys.argv[1],launcher_args=sys.argv[2:])
  else:
    launcher, launcher_args, run_cmd, restart_cmd, restart_args = parseargs(sys.argv[1:])
    scr_run(launcher_args=launcher_args,run_cmd=run_cmd,restart_cmd=restart_cmd,restart_args=restart_args)
