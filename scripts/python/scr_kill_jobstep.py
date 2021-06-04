#! /usr/bin/env python

# scr_kill_jobstep.py

# This script uses the scancel command to kill the job step with the 
# job step id supplied on the command line

def print_usage(prog):
  print('')
  print('  Usage:  $prog -j <jobstepid>')
  print('')
  print('    -j, --jobStepId    The job step id to kill.')
  print('')

def scr_kill_jobstep(argv):
  prog = 'scr_kill_jobstep'
  bindir = '@X_BINDIR@'

  killCmd = 'scancel'

  # read in the command line options
  jobid=None
  argv = argv.split(' ')
  for i in range(len(argv)):
    if argv[i]=='--jobStepId' or argv[i]=='-j':
      if i+1<len(argv):
        jobid=argv[i+1]
        break
    elif '=' in argv[i]:
      vals = argv[i].split('=')
      if vals[0]=='--jobStepId' or vals[0]=='-j:
        jobid=vals[1]
        break
    print_usage(prog)
    return 1
  if jobid==None:
    print('You must specify the job step id to kill.')
    print_usage(prog)
    return 1

  cmd = killCmd+' '+jobid
  print(cmd)
  argv = [killCmd,jobid]
  runproc = subprocess.Popen(args=argv, bufsize=1, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
  out = runproc.communicate()[0]
  return runproc.returncode