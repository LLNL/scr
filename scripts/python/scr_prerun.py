#! /usr/bin/env python

# SCR Pre Run

import os, sys, subprocess
from datetime import datetime

# print usage and exit
def print_usage(prog):
  print('Usage: '+prog+' [-p prefix_dir]')
  sys.exit(1)

# for verbose, print func():linenum -> event
def tracefunction(frame,event,arg):
  print(str(frame.f_code.co_name)+'():'+str(frame.f_lineno)+' -> '+str(event)+'\n')

val = os.environ.get('SCR_ENABLE')
if val is None or val == '0':
  sys.exit(0)
val = os.environ.get('SCR_DEBUG')
# enable verbosity
if val is not None and int(val) > 0:
  sys.settrace(tracefunction)

start_time = datetime.now()

###
bindir="@X_BINDIR@"
###

prog="scr_prerun"

# process command line options
pardir=bindir+'/scr_prefix'

val = False
for i in range(1,len(sys.argv)):
  if val==True:
    val = False
    pardir = os.environ.get('OPTARG') ### not sure if this is equivalent
  elif sys.argv[i]=='-p':
    val=True
  else:
    print_usage(prog)

# check that we have the parallel file system prefix directory
if pardir=="":
  print_usage(prog)

print(prog+': Started: +'str(start_time))

# this value is never used
ret=0

# check that we have all the runtime dependences we need
# CALL
#python $bindir/scr_test_runtime
# if return != 0 then print $prog: exit code: 1, exit(1)

# create the .scr subdirectory in the prefix directory
#mkdir -p ${pardir}/.scr
argv = ['mkdir','-p',pardir+'/.scr']
runproc = subprocess.Popen(args=argv)
out = runproc.communicate()

# TODO: It would be nice to clear the cache and control directories
# here in preparation for the run.  However, a simple rm -rf is too
# dangerous, since it's too easy to accidentally specify the wrong
# base directory.
#
# For now, we just keep this script around as a place holder.

# clear any existing flush or nodes files
# NOTE: we *do not* clear the halt file, since the user may have
# requested the job to halt
argv = ['rm','-f',pardir+'/.scr/flush.scr']
runproc = subprocess.Popen(args=argv)
out = runproc.communicate()
#rm -f ${pardir}/.scr/flush.scr
argv[2]=pardir+'/.scr/nodes.scr'
runproc = subprocess.Popen(args=argv)
out = runproc.communicate()
#rm -f ${pardir}/.scr/nodes.scr

# report timing info
end_time = datetime.now()
run_secs = end_time-start_time
print(prog+': Ended: '+str(end_time))
print(prog+': secs: '+str(run_secs.seconds))

# report exit code and exit
print(prog+': exit code: '+str(ret))
# (this exit code was never changed in original scripts/common/scr_prerun.in)
sys.exit(ret)

