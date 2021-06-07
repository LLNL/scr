#! /usr/bin/env python

# SCR Pre Run

import os, sys, subprocess, scr_const
from datetime import datetime
from scr_common import tracefunction
from scr_test_runtime import scr_test_runtime

# print usage and exit
def print_usage(prog):
  print('Usage: '+prog+' [-p prefix_dir]')

def scr_prerun(args):
  val = os.environ.get('SCR_ENABLE')
  if val is None or val == '0':
    return 0 # doesn't stop run when called from scr_run
  val = os.environ.get('SCR_DEBUG')
  # enable verbosity
  if val is not None and int(val) > 0:
    sys.settrace(tracefunction)

  start_time = datetime.now()
  bindir=scr_const.X_BINDIR
  prog='scr_prerun'

  # process command line options
  pardir=bindir+'/scr_prefix'

  if len(args)==1:
    pardir = args[0]
  elif len(args)==2 and args[0]=='-p':
    pardir = args[1]
    #pardir = os.environ.get('OPTARG') ### not sure what this was getting ###
  else:
    print_usage(prog)
    return 1
  print(prog+': Started: '+str(start_time))

  # check that we have all the runtime dependences we need
  if scr_test_runtime() != 0:
    print(prog+': exit code: 1')
    return 1

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
  print(prog+': exit code: 0')
  return 0
