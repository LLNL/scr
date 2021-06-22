#! /usr/bin/env python

from datetime import datetime
from time import time
import os, signal, sys, time, scr_const
import scr_common
from scr_common import tracefunction, runproc, scr_prefix
from scr_test_runtime import scr_test_runtime
from scr_list_dir import scr_list_dir
from scr_prerun import scr_prerun
from scr_get_jobstep_id import scr_get_jobstep_id
from scr_watchdog import scr_watchdog
from scr_list_down_nodes import scr_list_down_nodes
from scr_postrun import scr_postrun
from multiprocessing import Process
from env.scr_env import SCR_Env
from scr_param import SCR_Param
from scr_glob_hosts import scr_glob_hosts

def scr_run(launcher_args=[],run_cmd='',restart_cmd='',restart_args=[]):
  launcher='srun'
  prog='scr_'+launcher

  libdir=scr_const.X_LIBDIR
  bindir=scr_const.X_BINDIR

  val = os.environ.get('SCR_ENABLE')
  if val is not None and val=='0':
    launcher = [launcher]
    launcher.extend(launcher_args)
    returncode = runproc(argv=launcher)[1]
    sys.exit(returncode)

  #launcher_args = ' '.join(launcher_args)

  # turn on verbosity
  val = os.environ.get('SCR_DEBUG')
  if val is not None and int(val)>0:
    sys.settrace(tracefunction)

  # make a record of start time
  timestamp=datetime.now()
  start_secs = int(secs)
  print(prog+': Started: '+str(timestamp)+' ('+str(start_secs)+')')

  # check that we have runtime dependencies
  if scr_test_runtime()!=0:
    print(prog+': exit code: 1')
    sys.exit(1)

  # TODO: if not in job allocation, bail out

  scr_env = SCR_Env()
  jobid = scr_env.getjobid()

  # TODO: check that we have a valid jobid and bail if not

  # get the nodeset of this job
  nodelist = os.environ.get('SCR_NODELIST')
  if nodelist is None:
    nodelist = scr_env.getnodelist()
    if nodelist is None:
      print(prog+': ERROR: Could not identify nodeset')
      sys.exit(1)
    os.environ['SCR_NODELIST'] = nodelist

  # get prefix directory
  prefix=scr_prefix()
  scr_env.set_prefix(prefix)

  use_scr_watchdog=os.environ.get('SCR_WATCHDOG')
  if use_scr_watchdog is None or use_scr_watchdog!='1':
    use_scr_watchdog=False
  else:
    use_scr_watchdog=True

  # get the control directory
  cntldir = scr_list_dir('control',src_env)
  if type(cntldir) is not str:
    print(prog+': ERROR: Invalid control directory '+str(cntldir)+'.')
    sys.exit(1)

  # NOP srun to force every node to run prolog to delete files from cache
  # TODO: remove this if admins find a better place to clear cache
  argv=['srun','/bin/hostname'] # ,'>','/dev/null']

  runproc(argv=argv)

  # make a record of time prerun is started
  timestamp=datetime.now()
  print(prog+': prerun: '+str(timestamp))

  if scr_prerun(prefix=prefix)!=0:
    print(prog+': ERROR: Command failed: scr_prerun -p '+prefix)
    sys.exit(1)

  #export SCR_END_TIME=$(date -d $(scontrol --oneliner show job $SLURM_JOBID | perl -n -e 'm/EndTime=(\S*)/ and print $1') +%s)
  val = os.environ.get('SLURM_JOBID')
  endtime = ''
  if val is not None:
    argv=[ ['scontrol','--oneliner','show','job',val], ['perl','-n','-e','\'m/EndTime=(\S*)/ and print $1\''] ]
    endtime = pipeproc(argvs=argv,getstdout=True)[0]
    argv = ['date','-d',endtime]
    endtime = runprov(argv=argv,getstdout=True)[0]
  else:
    print(prog+': WARNING: Unable to get end time.') # shouldn't happen

  os.environ['SCR_END_TIME'] = endtime

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
    exclude=''
    down_nodes = scr_list_down_nodes(free=free_flag,nodeset_down=down_nodes,scr_env=scr_env)
    if type(down_nodes) is str and down_nodes!='':
      # print the reason for the down nodes, and log them
      scr_list_down_nodes(reason=True, free=free_flag, nodeset_down=down_nodes, log_nodes=True, runtime_secs='0', scr_env=scr_env)

      # if this is the first run, we hit down nodes right off the bat, make a record of them
      if attempts==0:
        start_secs=int(time())
        print('SCR: Failed node detected: JOBID='+jobid+' ATTEMPT=0 TIME='+str(start_secs)+' NNODES=-1 RUNTIME=0 FAILED='+down_nodes)

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
          num_needed = scr_glob_hosts(['--count','--hosts',nodelist])

      # check that we have enough nodes left to run the job after excluding all down nodes
      num_left = scr_glob_hosts(['--count','--minus',nodelist+':'+down_nodes])
      # num_left='$bindir/scr_glob_hosts --count --minus $SCR_NODELIST:$down_nodes'
      if num_left < num_needed:
        print(prog+': (Nodes remaining='+num_left+') < (Nodes needed='+num_needed+'), ending run.')
        break

      # all checks pass, exclude the down nodes and continue
      exclude=['--exclude', down_nodes]

    # make a record of when each run is started
    attempts+=1
    timestamp=datetime.now()
    print(prog+': RUN '+str(attempts)+': '+str(timestamp))

    launch_cmd=launcher_args.copy()
    if restart_cmd!='' and os.path.isfile(restart_cmd) and os.access(restart_cmd,os.X_OK):
      restart_name=launcher+' '+' '.join(launcher_args)+' '+bindir+'/scr_have_restart'
      if os.path.isfile(restart_name) and os.acess(restart_name,os.X_OK):
        my_restart_cmd='echo '+restart_cmd+' '+bindir+'/scr_have_restart'
        my_restart_cmd = re.sub('SCR_CKPT_NAME',restart_name,my_restart_cmd)
        launch_cmd.append(my_restart_cmd)
      else:
        launch_cmd.append(run_cmd)
    else:
      launch_cmd.append(run_cmd)
    # launch the job, make sure we include the script node and exclude down nodes
    start_secs=int(time())

    scr_common.log(bindir=bindir, prefix=prefix, jobid=jobid, event_type='RUN_START', event_note='run='+str(attempts), event_start=str(start_secs))
    # $bindir/scr_log_event -i $jobid -p $prefix -T "RUN_START" -N "run=$attempts" -S $start_secs
    if use_scr_watchdog == False:
      argv=[launcher]
      argv.extend(exclude)
      argv.extend(launch_cmd)
      runproc(argv=argv)
      # $launcher $exclude $launch_cmd
    else:
      print(prog+': Attempting to start watchdog process.')
      # need to get job step id of the srun command
      argv=[launcher]
      argv.extend(exclude)
      argv.extend(launch_cmd)
      srun_pid = runproc(argv=argv, wait=False)[1]
      # $launcher $exclude $launch_cmd &
      #srun_pid = runproc.pid
      #srun_pid=$!;
      time.sleep(10)
      #sleep 10; # sleep a bit to wait for the job to show up in squeue
      print(bindir+'/scr_get_jobstep_id '+str(srun_pid))
      jobstepid = scr_get_jobstep_id(scr_env) # the pid was unused in there
      # then start the watchdog  if we got a valid job step id
      if jobstepid is not None:
        # Launching a new process to execute the python method
        watchdog = Process(target=scr_watchdog,args=('--dir',prefix,'--jobStepId',jobstepid))
        watchdog.start()
        print(prog+': Started watchdog process with PID '+str(watchdog.pid)+'.')
      else:
        print(prog+': ERROR: Unable to start scr_watchdog because couldn\'t get job step id.')
      runproc.check_call()

    end_secs=int(time())
    run_secs=end_secs - start_secs

    # check for and log any down nodes
    scr_list_down_nodes(reason=True, nodeset_down=keep_down, log_nodes=True, runtime_secs=str(run_secs), scr_env=scr_env)
    # log stats on the latest run attempt
    scr_common.log(bindir=bindir, prefix=prefix, jobid=jobid, event_type='RUN_END', event_note='run='+str(attempts), event_start=str(end_secs), event_secs=str(run_secs))
    #$bindir/scr_log_event -i $jobid -p $prefix -T "RUN_END" -N "run=$attempts" -S $end_secs -L $run_secs

    # any retry attempts left?
    if runs > 1:
      runs-=1
      if runs <= 0:
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
  if scr_postrun(prefix=prefix,scr_env=scr_env) != 0:
    print(prog+': ERROR: Command failed: scr_postrun -p '+prefix)

  # kill the watchdog process if it is running
  if watchdog is not None and watchdog.is_alive():
    print('Killing watchdog using kill -SIGKILL '+str(watchdog.pid))
    os.kill(watchdog.pid, signal.SIGKILL)

  # make a record of end time
  timestamp=datetime.now()
  print(prog+': Ended: '+str(timestamp))

def print_usage(launcher):
  print('USAGE:')
  # from original parsing it looks like all launcher args are combined
  # (no matter whether they appear in the front or at the end)
  # the original usage printing looks like you could define different launcher args
  # (or define launcher args for only initial / restart and other without args, etc.)
  print('scr_'+launcher+' ['+launcher+' args] [-rc|--run-cmd=<run_command>] [-rs|--restart-cmd=<restart_command>]') # ['+launcher+' args]')
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

if __name__=='__main__':
  # argparse doesn't handle parsing of mixed positionals
  # there is a parse_intermixed_args which requires python 3.7 which may work
  # I'm just going to skip the argparse
  launcher_args = []
  run_cmd, restart_cmd = '', ''
  restart_args = []
  position = 0
  for i in range(1,len(sys.argv)):
    # just printing help, print the help and exit
    if sys.argv[i] == '-h' or sys.argv[i] == '--help':
      print_usage('scr_run')
      sys.exit(0)
    # haven't gotten a run command yet
    if position==0:
      # run command is next (or here if we have an '=')
      if sys.argv[i].startswith('-rc') or sys.argv[i].startswith('--run-cmd'):
        position+=1
        if '=' in sys.argv[i]:
          run_cmd = sys.argv[i].split('=')[1]
          position+=1
      # no run command, just a restart command
      elif sys.argv[i].startswith('-rs') or sys.argv[i].startswith('--restart-cmd'):
        position=3
        if '=' in sys.argv[i]:
          restart_cmd = sys.argv[i].split('=')[1]
          position+=1
      # these are launcher args before the run command
      else:
        launcher_args.append(sys.argv[i])
    # position was 1, this is now our run command
    elif position==1:
      run_cmd = sys.argv[i]
      position+=1
    # we've gotten our run command, next is the restart command
    elif position==2:
      position+=1
      if '=' in sys.argv[i]:
        restart_cmd = sys.argv[i].split('=')[1]
        position+=1
    # get the restart command
    elif position==3:
      position+=1
      restart_cmd = sys.argv[i]
    # these are trailing restart args
    else:
      restart_args.append(sys.argv[i])
  scr_run(launcher_args=launcher_args,run_cmd=run_cmd,restart_cmd=restart_cmd,restart_args=restart_args)
