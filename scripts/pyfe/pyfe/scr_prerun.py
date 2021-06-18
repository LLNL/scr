#! /usr/bin/env python

# SCR Pre Run

import argparse, os, sys, subprocess, scr_const
from datetime import datetime
from time import time
from scr_common import tracefunction, scr_prefix
from scr_test_runtime import scr_test_runtime

def scr_prerun(prefix=None):
  val = os.environ.get('SCR_ENABLE')
  if val is None or val == '0':
    return 0 # doesn't stop run when called from scr_run
  val = os.environ.get('SCR_DEBUG')
  # enable verbosity
  if val is not None and int(val) > 0:
    sys.settrace(tracefunction)

  start_time = datetime.now()
  start_secs = int(time())
  bindir=scr_const.X_BINDIR
  prog='scr_prerun'

  # process command line options
  pardir=''
  #pardir = os.environ.get('OPTARG') ### not sure what this was getting ###
  if prefix is not None:
    pardir=prefix
  else:
    pardir=scr_prefix()

  print(prog+': Started: '+str(start_time))

  # check that we have all the runtime dependences we need
  if scr_test_runtime() != 0:
    print(prog+': exit code: 1')
    return 1

  # create the .scr subdirectory in the prefix directory
  #mkdir -p ${pardir}/.scr
  os.makedirs(pardir+'/.scr',exist_ok=True)

  # TODO: It would be nice to clear the cache and control directories
  # here in preparation for the run.  However, a simple rm -rf is too
  # dangerous, since it's too easy to accidentally specify the wrong
  # base directory.
  #
  # For now, we just keep this script around as a place holder.

  # clear any existing flush or nodes files
  # NOTE: we *do not* clear the halt file, since the user may have
  # requested the job to halt
  # remove files: ${pardir}/.scr/{flush.scr,nodes.scr}
  try:
    os.remove(pardir+'/.scr/flush.scr')
  except: # error on doesn't exist / etc ...
    pass
  try:
    os.remove(pardir+'/.scr/nodes.scr')
  except:
    pass

  # report timing info
  end_time = datetime.now()
  run_secs = int(time())-start_time
  print(prog+': Ended: '+str(end_time))
  print(prog+': secs: '+str(run_secs))

  # report exit code and exit
  print(prog+': exit code: 0')
  return 0

if __name__=='__main__':
  parser = argparse.ArgumentParser(add_help=False, argument_default=argparse.SUPPRESS, prog='scr_prerun')
  parser.add_argument('-h','--help', action='store_true', help='Show this help message and exit.')
  parser.add_argument('-p','--prefix', metavar='<dir>', type=str, default=None, help='Specify the prefix directory.')
  args = vars(parser.parse_args())
  if 'help' in args:
    parser.print_help()
  #elif args['prefix'] is None:
  #  print('The prefix directory must be specified.')
  else:
    ret = scr_prerun(args['prefix'])

