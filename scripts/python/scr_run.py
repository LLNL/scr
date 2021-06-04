#! /usr/bin/env python

from datetime import datetime
import os, psutil, signal, sys, time
import scr_common
from multiprocessing import Process
from scr_env import SCR_Env
from scr_param import SCR_Param

### This is the compile-time constants ###
### this should happen somewhere else ###
import compileconsts
compileconsts.compileconsts(vals)

### This is common to several files ###

launcher='srun'
prog='scr_'+launcher

libdir='@X_LIBDIR@'
bindir='@X_BINDIR@'

def printusage():
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
  sys.exit(0)

# capture restart and run commands if specified
# parse argv takes sys.argv[2:] (starting past script+launcher names), returns command line options
def parseargv(argv):
  run_cmd = ''
  restart_cmd=''
  launcher_args = []
  skip=0
  for i in range(len(argv)):
    if skip!=0:
      skip=0
      continue
    elif argv[i]=='--restart-cmd' or argv[i]=='-rs':
      if i+1==len(argv):
        printusage()
      restart_cmd=argv[i+1]
      skip=1
    elif argv[i]=='--run-cmd' or argv[i]=='-rc':
      if i+1==len(argv):
        printusage()
      run_cmd=argv[i+1]
      skip=1
    elif argv[i].startswith('--restart-cmd=') or argv[i].startswith('-rs='):
      restart_cmd=argv[i].split('=')[1]
    elif argv[i].startswith('--run-cmd=') or argv[i].startswith('-rc='):
      run_cmd=argv[i].split('=')[1]
    else:
      launcher_args.append(argv[i])
  return run_cmd, restart_cmd, launcher_args

run_cmd=''
restart_cmd=''
launcher_args=[]

if len(sys.argv)<2:
  printusage()
elif len(sys.argv)>2:
  run_cmd, restart_cmd, launcher_args = parseargv(sys.argv[2:])

val = os.environ.get('SCR_ENABLE')
if val is not None and val=='0':
  argv = [launcher]
  argv.extend(launcher_args)
  runproc = subprocess.Popen(args=argv)
  runproc.communicate()
  sys.exit(runproc.returncode)

# turn on verbosity
val = os.environ.get('SCR_DEBUG')
if val is not None and int(val)>0:
  sys.settrace(scr_common.tracefunction)

# make a record of start time
timestamp=datetime.now()
print(prog+': Started: '+str(timestamp))

# check that we have runtime dependencies
if scr_common.scr_test_runtime()!=0:
  print(prog+': exit code: 1')
  sys.exit(1)

# TODO: if not in job allocation, bail out

scr_env = SCR_Env('SLURM') ### dynamically get the environment ###
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
prefix=bindir+'/scr_prefix'
scr_env.set_prefix(prefix)

use_scr_watchdog=os.environ.get('SCR_WATCHDOG')
if use_scr_watchdog is None or use_scr_watchdog!='1':
  use_scr_watchdog='0'

# get the control directory
cntldir = scr_common.scr_list_dir('control',src_env)
if type(cntldir) is not str:
  print(prog+': ERROR: Invalid control directory '+str(cntldir)+'.')
  sys.exit(1)

# NOP srun to force every node to run prolog to delete files from cache
# TODO: remove this if admins find a better place to clear cache
argv=['srun','/bin/hostname'] # ,'>','/dev/null']
runproc = subprocess.Popen(args=argv,stdin=None,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
out,err = runproc.communicate() # we capture out/err, just ignore it

# make a record of time prerun is started
timestamp=datetime.now()
print(prog+': prerun: '+str(timestamp))

if scr_common.scr_prerun(prefix)!=0:
  print(prog+': ERROR: Command failed: scr_prerun -p '+prefix)
  sys.exit(1)

val = os.environ.get('SLURM_JOBID')
argv=['scontrol','--oneliner','show','job',val]
runproc = subprocess.Popen(args=argv, bufsize=1, stdin=None, 
stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, 
universal_newlines=True)
argv=['perl','-n','-e','m/EndTime=(\S*)/ and print $1']
runproc = subprocess.Popen(args=argv, bufsize=1, stdin=runproc, 
stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, 
universal_newlines=True)
out,err = runproc.communicate()
argv=['date','-d',out,'+%s']
runproc = subprocess.Popen(args=argv, bufsize=1, stdin=None, 
stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, 
universal_newlines=True)
out,err = runproc.communicate()
os.environ['SCR_END_TIME'] = out

#export SCR_END_TIME=$(date -d $(scontrol --oneliner show job $SLURM_JOBID | 
perl -n -e 'm/EndTime=(\S*)/ and print $1') +%s)

# enter the run loop
down_nodes=''
attempts=0
runs = os.environ.get('SCR_RUNS')
if runs is None:
  runs = os.environ.get('SCR_RETRIES')
  if runs is None:
    runs=1
  else:
    runs=int(runs)+1
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
  keep_down=''
  if down_nodes!='':
    keep_down='--down '+down_nodes

  # if this is our first run, check that the free space on the drive meets requirement
  # (make sure data from job of previous user was cleaned up ok)
  # otherwise, we'll just check the total capacity
  free_flag=''
  if attempts == 0:
    free_flag='--free'

  # are there enough nodes to continue?
  exclude=''
  argv=[bindir+'/scr_list_down_nodes',free_flag,keep_down]
  runproc = subprocess.Popen(args=argv, bufsize=1, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
  #  down_nodes=bindir+'/scr_list_down_nodes '+free_flag+' '+keep_down
  down_nodes = runproc.communicate()[0]
  if down_nodes!='':
    # print the reason for the down nodes, and log them
    argv=[bindir+'/scr_list_down_nodes',free_flag,keep_down,'--log','--reason','--secs','0']
    runproc = subprocess.Popen(args=argv, bufsize=1, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
    # $bindir/scr_list_down_nodes $free_flag $keep_down --log --reason --secs 0
    runproc.communicate()

    # if this is the first run, we hit down nodes right off the bat, make a record of them
    if attempts==0:
      start_secs=datetime.now()
      print('SCR: Failed node detected: JOBID='+jobid+' ATTEMPT=0 TIME='+str(start_secs)+' NNODES=-1 RUNTIME=0 FAILED='+down_nodes)

    # determine how many nodes are needed:
    #   if SCR_MIN_NODES is set, use that
    #   otherwise, use value in nodes file if one exists
    #   otherwise, assume we need all nodes in the allocation
    # to start, assume we need all nodes in the allocation
    # if SCR_MIN_NODES is set, use that
    num_needed = os.environ.get('SCR_MIN_NODES')
    if num_needed is None:
      # try to lookup the number of nodes used in the last run
      argv=[bindir+'/scr_env','--prefix',prefix,'--runnodes']
      runproc = subprocess.Popen(args=argv, bufsize=1, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
      # num_needed_env='$bindir/scr_env --prefix $prefix --runnodes'
      num_needed_env = int(runproc.communicate()[0])
      if runproc.returncode==0:
        if num_needed_env>0:
          # if the command worked, and the number is something larger than 0, go with that
          num_needed=num_needed_env
    if num_needed is None:
      argv=[bindir+'/scr_glob_hosts','--count','--hosts',nodelist]
      runproc = subprocess.Popen(args=argv, bufsize=1, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
      # num_needed='$bindir/scr_glob_hosts --count --hosts $SCR_NODELIST'
      num_needed = int(runproc.communicate()[0])

    # check that we have enough nodes left to run the job after excluding all down nodes
    argv=[bindir+'/scr_glob_hosts','--count','--minus',nodelist+':'+down_nodes]
    runproc = subprocess.Popen(args=argv, bufsize=1, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
    # num_left='$bindir/scr_glob_hosts --count --minus $SCR_NODELIST:$down_nodes'
    num_left = int(runproc.communicate()[0])
    if num_left is None or num_left < num_needed:
      print(prog+': (Nodes remaining='+num_left+') < (Nodes needed='+num_needed+'), ending run.')
      break

    # all checks pass, exclude the down nodes and continue
    exclude='--exclude '+down_nodes

  # make a record of when each run is started
  attempts+=1
  timestamp=datetime.now()
  print(prog+': RUN '+str(attempts)+': '+str(timestamp))

  launch_cmd=launcher_args
  launch_cmd.extend(run_cmd)
'''
{{{}}}
'''
  if os.path.isfile(restart_cmd) and os.access(restart_cmd,os.X_OK):
    restart_name=launcher+' '+' '.join(launcher_args)+' '+bindir+'/scr_have_restart'
    if os.path.isfile(restart_name) and os.acess(restart_name,os.X_OK):
      my_restart_cmd='echo '+restart_cmd+' '+bindir+'/scr_have_restart'
      launch_cmd=' '.join(launcher_args)+' '+myrestart_cmd
####
  if [ ${restart_cmd:+x} ]; then
      restart_name='$launcher $launcher_args $bindir/scr_have_restart'
      if [ ${restart_name:+x} ]; then
          my_restart_cmd='echo $restart_cmd | sed "s#SCR_CKPT_NAME#${restart_name}#g"'
          launch_cmd="$launcher_args $my_restart_cmd"
      fi
  fi
'''
{{{}}}
'''
  # launch the job, make sure we include the script node and exclude down nodes
  start_secs=datetime.now()

  $bindir/scr_log_event -i $jobid -p $prefix -T "RUN_START" -N "run=$attempts" -S $start_secs
  if use_scr_watchdog == '0':
    argv=[launcher]
    argv.extend(exclude)
    argv.extend(launch_cmd)
    runproc = subprocess.Popen(args=argv)
    # $launcher $exclude $launch_cmd
    runproc.communicate()
  else:
    print(prog+': Attempting to start watchdog process.')
    # need to get job step id of the srun command
    argv=[launcher]
    argv.extend(exclude)
    argv.extend(launch_cmd)
    runproc = subprocess.Popen(args=argv)
    # $launcher $exclude $launch_cmd &
	srun_pid = runproc.pid
    #srun_pid=$!;
    time.sleep(10)
    #sleep 10; # sleep a bit to wait for the job to show up in squeue
    jobstepid = scr_common.scr_get_jobstep_id(scr_env,srun_pid)
    # then start the watchdog  if we got a valid job step id
    if jobstepid!='' and jobstepid!='-1':
      watchdog = Process(target=scr_common.scr_watchdog,args=('--dir',prefix,'--jobStepId',jobstepid))
      watchdog.start()
      print(prog+': Started watchdog process with PID '+str(watchdog.pid)+'.')
    else:
      print(prog+': ERROR: Unable to start scr_watchdog because couldn\'t get job step id.')
    out = runproc.communicate()

  end_secs=datetime.now()
  run_secs=end_secs - start_secs

  # check for and log any down nodes
  scr_common.scr_list_down_nodes([keep_down,'--log','--reason','--secs',str(run_secs.seconds)],scr_env)
  # log stats on the latest run attempt
  argv=[bindir+'/scr_log_event','-i',jobid,'-p',prefix,'-T','RUN_END','-N','run='+str(attempts),'-S',end_secs,'-L',str(run_secs.seconds)]
  runproc = subprocess.Popen(args=argv, bufsize=1, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
  runproc.communicate()
  #$bindir/scr_log_event -i $jobid -p $prefix -T "RUN_END" -N "run=$attempts" -S $end_secs -L $run_secs

  # any retry attempts left?
  if runs > 1:
    runs-=1
  else:
    runs = os.environ.get('SCR_RUNS')
    print(prog+': '+runs+' exhausted, ending run.')
    break

'''
{}{}{}
'''
  # is there a halt condition instructing us to stop?
  argv=[bindir+'/scr_retries_halt','--dir',prefix]
  runproc = subprocess.Popen(args=argv, bufsize=1, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
  runproc.communicate()
  #$bindir/scr_retries_halt --dir $prefix;
  if runproc.returncode==0:
    print(prog+': Halt condition detected, ending run.')
    break

  # give nodes a chance to clean up
  time.sleep(60)

  # check for halt condition again after sleep
  runproc = subprocess.Popen(args=argv, bufsize=1, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
  runproc.communicate()
  #$bindir/scr_retries_halt --dir $prefix;
  if runproc.returncode==0:
    print(prog+': Halt condition detected, ending run.')
    break

# make a record of time postrun is started
timestamp=datetime.now()
print(prog+': postrun: '+str(timestamp))

# scavenge files from cache to parallel file system
if scr_common.scr_postrun(['-p',prefix]) != 0:
  print(prog+': ERROR: Command failed: scr_postrun -p '+prefix)

# kill the watchdog process if it is running
if watchdog is not None and watchdog.is_alive():
  print('Killing watchdog using kill -SIGKILL '+str(watchdog.pid))
  psutil.Process(watchdog.pid).send_signal(signal.SIGKILL)

# make a record of end time
timestamp=datetime.now()
print(prog+': Ended: '+str(timestamp))
