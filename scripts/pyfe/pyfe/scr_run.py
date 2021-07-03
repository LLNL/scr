#! /usr/bin/env python3

# scr_run.py

# the general launcher for the scripts
# if called directly the launcher to use (srun/jsrun/mpirun) should be specified as an argument
# scr_{srun,jsrun,mpirun} scripts call this script with the launcher specified

from datetime import datetime
import os, signal, sys, time # need to use both time.time() and time.sleep()
import multiprocessing as mp
from pyfe import scr_const, scr_common
from pyfe.scr_common import tracefunction, runproc, scr_prefix
from pyfe.scr_test_runtime import scr_test_runtime
from pyfe.scr_list_dir import scr_list_dir
from pyfe.scr_prerun import scr_prerun
from pyfe.scr_get_jobstep_id import scr_get_jobstep_id
from pyfe.scr_watchdog import scr_watchdog
from pyfe.scr_list_down_nodes import scr_list_down_nodes
from pyfe.scr_postrun import scr_postrun
from pyfe.scr_env import SCR_Env
from pyfe.joblauncher.scr_joblauncher import SCR_Joblauncher
from pyfe.resmgr import AutoResourceManager
from pyfe.scr_param import SCR_Param
from pyfe.scr_glob_hosts import scr_glob_hosts

def scr_run(launcher='',launcher_args=[],run_cmd='',restart_cmd='',restart_args=[]):
  if launcher=='':
    launcher=scr_const.SCR_LAUNCHER
    if '@' in launcher:
      print('Launcher must be specified')
      return 1
  prog='scr_'+launcher

  libdir=scr_const.X_LIBDIR
  bindir=scr_const.X_BINDIR

  val = os.environ.get('SCR_ENABLE')
  if val is not None and val=='0':
    launcher = [launcher]
    launcher.extend(launcher_args)
    returncode = runproc(argv=launcher)[1]
    sys.exit(returncode)

  # turn on verbosity
  val = os.environ.get('SCR_DEBUG')
  verbose = False
  if val is not None and int(val)>0:
    verbose=True
    sys.settrace(tracefunction)

  # make a record of start time
  timestamp=datetime.now()
  start_secs = int(time.time())
  print(prog+': Started: '+str(timestamp))

  # check that we have runtime dependencies
  if scr_test_runtime()!=0:
    print('scr_test_runtime returned a failure')
    print(prog+': exit code: 1')
    sys.exit(1)

  # TODO: if not in job allocation, bail out

  param = SCR_Param()
  scr_env = SCR_Env() # env contains general environment infos independent of resmgr/launcher
  resourcemgr = AutoResourceManager() # resource manager (SLURM/LSF/ ...) set by argument or compile constant
  launcher = SCR_Joblauncher(launcher) # launcher contains attributes unique to launcher (srun/jsrun/ ...)
  # give scr_env a pointer to the objects for calling other methods
  scr_env.param = param
  scr_env.resmgr = resourcemgr
  scr_env.launcher = launcher
  # this may be used by a launcher to store a list of hosts
  scr_env.launcher.conf['hostfile'] = scr_env.conf['prefix']+'/.scr/hostfile'
  # jobid will come from resource manager.
  jobid = resourcemgr.conf['jobid']

  # TODO: check that we have a valid jobid and bail if not
  # pmix always returns None
  # others return None when it can't be found
  # previously they returned 'defjobid' with the comment to assume testing
  if jobid is None: #### pmix always returns none.
    jobid = 'defjobid'
    #print(prog+': ERROR: Could not determine jobid.')
    #sys.exit(1)

  # get the nodeset of this job
  nodelist = scr_env.conf['nodes']
  if nodelist is None:
    nodelist = scr_env.resmgr.conf['nodes']
    if nodelist is None:
      print(prog+': ERROR: Could not identify nodeset')
      sys.exit(1)

  # get prefix directory
  prefix=scr_env.conf['prefix']

  val=os.environ.get('SCR_WATCHDOG')
  if val is None or val!='1':
    resourcemgr.usewatchdog(False)
  else:
    resourcemgr.usewatchdog(True)

  # get the control directory
  cntldir = scr_list_dir(user=scr_env.conf['user'],jobid=resourcemgr.conf['jobid'],runcmd='control',scr_env=scr_env)
  if cntldir == 1:
    print(prog+': ERROR: Could not determine control directory')
    sys.exit(1)

  # run a NOP with srun, other launchers could do any preamble work here
  launcher.prepareforprerun()

  # make a record of time prerun is started
  timestamp=datetime.now()
  print(prog+': prerun: '+str(timestamp))

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

  # need to reference the watchdog process outside of this loop
  watchdog = None

  # set the starting method for the mp.Process() call
  if resourcemgr.usewatchdog() == True:
    mp.set_start_method('forkserver') # https://docs.python.org/3/library/multiprocessing.html
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
    down_nodes = scr_list_down_nodes(free=free_flag,nodeset_down=down_nodes,scr_env=scr_env)
    # returns 0 for none, 1 for error, or a string
    # could handle error here
    if type(down_nodes) is int:
      down_nodes = ''
    if down_nodes!='':
      # print the reason for the down nodes, and log them
      scr_list_down_nodes(reason=True, free=free_flag, nodeset_down=down_nodes, log_nodes=True, runtime_secs='0', scr_env=scr_env)

      # if this is the first run, we hit down nodes right off the bat, make a record of them
      if attempts==0:
        start_secs=int(time.time())
        print('SCR: Failed node detected: JOBID='+jobid+' ATTEMPT=0 TIME='+str(start_secs)+' NNODES=-1 RUNTIME=0 FAILED='+down_nodes)

      ##### Unless this value may change at some point in an allocation
      ##### This could be moved outside of this loop, this value is not changed within the loop
      ##### *  num_needed  *

      # determine how many nodes are needed:
      #   if SCR_MIN_NODES is set, use that
      #   otherwise, use value in nodes file if one exists
      #   otherwise, assume we need all nodes in the allocation
      # to start, assume we need all nodes in the allocation
      # if SCR_MIN_NODES is set, use that
      num_needed = os.environ.get('SCR_MIN_NODES')
      if num_needed is None or int(num_needed) <= 0:
        # try to lookup the number of nodes used in the last run
        num_needed = scr_env.get_runnode_count()
        # num_needed_env='$bindir/scr_env --prefix $prefix --runnodes'
        # if the command worked, and the number is something larger than 0, go with that
        if num_needed<=0:
          num_needed = scr_glob_hosts(count=True,hosts=nodelist)
          if num_needed is None:
            print(prog+': ERROR: Unable to determine number of nodes needed')
            break
      num_needed = int(num_needed)

      # check that we have enough nodes left to run the job after excluding all down nodes
      num_left = scr_glob_hosts(count=True,minus=nodelist+':'+down_nodes)
      if num_left is None:
        print(prog+': ERROR: Unable to determine number of nodes remaining')
        break
      num_left = int(num_left)
      # num_left='$bindir/scr_glob_hosts --count --minus $SCR_NODELIST:$down_nodes'
      if num_left < num_needed:
        print(prog+': (Nodes remaining='+str(num_left)+') < (Nodes needed='+str(num_needed)+'), ending run.')
        break

    # make a record of when each run is started
    attempts+=1
    timestamp=datetime.now()
    print(prog+': RUN '+str(attempts)+': '+str(timestamp))

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
    start_secs=int(time.time())

    scr_common.log(bindir=bindir, prefix=prefix, jobid=jobid, event_type='RUN_START', event_note='run='+str(attempts), event_start=str(start_secs))
    # $bindir/scr_log_event -i $jobid -p $prefix -T "RUN_START" -N "run=$attempts" -S $start_secs
    if resourcemgr.usewatchdog() == False:
      argv=launcher.getlaunchargv(up_nodes=nodelist,down_nodes=down_nodes,launcher_args=launch_cmd)
      runproc(argv=argv)
      # $launcher $exclude $launch_cmd
    else:
      print(prog+': Attempting to start watchdog process.')
      # need to get job step id of the srun command
      argv=launcher.getlaunchargv(up_nodes=nodelist,down_nodes=down_nodes,launcher_args=launch_cmd)
      launched_process, launcher_pid = runproc(argv=argv, wait=False)
      # $launcher $exclude $launch_cmd &
      #{launcher}run_pid = runproc.pid
      #{launcher}run_pid=$!;
      time.sleep(10)
      #sleep 10; # sleep a bit to wait for the job to show up in squeue
      print(bindir+'/scr_get_jobstep_id '+str(launcher_pid))
      jobstepid = scr_get_jobstep_id(scr_env=scr_env,pid=launcher_pid)
      # then start the watchdog  if we got a valid job step id
      if jobstepid is not None:
        # Launching a new process to execute the python method
        watchdog = mp.Process(target=scr_watchdog,kwargs={prefix:prefix, jobstepid:jobstepid, scr_env:scr_env})
        watchdog.start()
        print(prog+': Started watchdog process with PID '+str(watchdog.pid)+'.')
      else:
        print(prog+': ERROR: Unable to start scr_watchdog because couldn\'t get job step id.')
      # check_call will wait for the process to finish without trying to get other information from it
      launched_process.check_call()

    end_secs=int(time.time())
    run_secs=end_secs - start_secs

    # check for and log any down nodes
    scr_list_down_nodes(reason=True, nodeset_down=keep_down, log_nodes=True, runtime_secs=str(run_secs), scr_env=scr_env)
    # log stats on the latest run attempt
    scr_common.log(bindir=bindir, prefix=prefix, jobid=jobid, event_type='RUN_END', event_note='run='+str(attempts), event_start=str(end_secs), event_secs=str(run_secs))
    #$bindir/scr_log_event -i $jobid -p $prefix -T "RUN_END" -N "run=$attempts" -S $end_secs -L $run_secs

    # any retry attempts left?
    if runs > 1:
      runs-=1
      if runs == 0:
        runs = os.environ.get('SCR_RUNS')
        print(prog+': '+runs+' exhausted, ending run.')
        break

    # is there a halt condition instructing us to stop?
    argv=[bindir+'/scr_retries_halt','--dir',prefix]
    returncode = runproc(argv=argv)[1]
    #$bindir/scr_retries_halt --dir $prefix;
    if returncode==0:
      print(prog+': Halt condition detected, ending run.')
      break

    # give nodes a chance to clean up
    time.sleep(60)

    # check for halt condition again after sleep
    returncode = runproc(argv=argv)[1]
    #$bindir/scr_retries_halt --dir $prefix;
    if returncode==0:
      print(prog+': Halt condition detected, ending run.')
      break

  # make a record of time postrun is started
  timestamp=datetime.now()
  print(prog+': postrun: '+str(timestamp))

  # scavenge files from cache to parallel file system
  if scr_postrun(prefix_dir=prefix,scr_env=scr_env) != 0:
    print(prog+': ERROR: Command failed: scr_postrun -p '+prefix)

  # kill the watchdog process if it is running
  if watchdog is not None and watchdog.is_alive():
    print('Killing watchdog using kill -SIGKILL '+str(watchdog.pid))
    os.kill(watchdog.pid, signal.SIGKILL)

  # make a record of end time
  timestamp=datetime.now()
  print(prog+': Ended: '+str(timestamp))

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
