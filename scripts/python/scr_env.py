#! /usr/bin/env python

# SCR Pre Run

import os, sys, subprocess
from datetime import datetime

# for verbose, print func():linenum -> event
def tracefunction(frame,event,arg):
  print(str(frame.f_code.co_name)+'():'+str(frame.f_lineno)+' -> '+str(event)+'\n')

val = os.environ.get('SCR_ENABLE')
if val is None or val == '0':
  sys.exit(0)
val = os.environ.get('SCR_DEBUG')
if val is not None and int(val) > 0:
  sys.settrace(tracefunction)

start_time = datetime.now()

###
bindir="@X_BINDIR@"
###

prog="scr_prerun"

def print_usage():
  print(f'Usage: {prog} [-p prefix_dir]')
  sys.exit(1)

# process command line options
pardir=bindir+'/scr_prefix'

val = False
for i in range(1,len(sys.argv)):
  if val==True:
    val=False
    pardir=sys.argv[i]
  elif sys.argv[i]=='-p':
    val=True
  elif sys.argv[i].startswith('-p') and len(sys.argv[i])>2:
    pardir=sys.argv[i][2:]
  else:
    print_usage()

# check that we have the parallel file system prefix directory
#if pardir=="":

print(f'{prog}: Started: {start_time}')

ret=0

# check that we have all the runtime dependences we need
# CALL
#python $bindir/scr_test_runtime
# if return != 0 then print $prog: exit code: 1, exit(1)

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
print(f'{prog}: Ended: {end_time}')
print(f'{prog}: secs: {run_secs.seconds}')

# report exit code and exit
print(f'{prog}: exit code: {ret}')
# (this exit code was never changed in original scripts/common/scr_prerun.in)
sys.exit(ret)

