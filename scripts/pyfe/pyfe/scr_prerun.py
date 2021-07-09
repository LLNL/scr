#! /usr/bin/env python3

# SCR Pre Run

import os, sys

if 'pyfe' not in sys.path:
  sys.path.insert(0,'/'.join(os.path.realpath(__file__).split('/')[:-2]))
  import pyfe

import argparse, subprocess
from datetime import datetime
from time import time
from pyfe import scr_const
from pyfe.scr_common import tracefunction, scr_prefix
from pyfe.scr_test_runtime import scr_test_runtime

def scr_prerun(prefix=None):
  val = os.environ.get('SCR_ENABLE')
  if val is None or val == '0':
    return 0
  val = os.environ.get('SCR_DEBUG')
  # enable verbosity
  if val is not None and int(val) > 0:
    sys.settrace(tracefunction)

  start_time = datetime.now()
  start_secs = int(time())
  bindir=scr_const.X_BINDIR

  pardir = scr_prefix() if prefix is None else prefix

  print('scr_prerun: Started: '+str(start_time))

  # check that we have all the runtime dependences we need
  if scr_test_runtime() != 0:
    print('scr_prerun: exit code: 1')
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
  run_secs = int(time())-start_secs
  print('scr_prerun: Ended: '+str(end_time))
  print('scr_prerun: secs: '+str(run_secs))

  # report exit code and exit
  print('scr_prerun: exit code: 0')
  return 0

if __name__=='__main__':
  parser = argparse.ArgumentParser(add_help=False, argument_default=argparse.SUPPRESS, prog='scr_prerun')
  parser.add_argument('-h','--help', action='store_true', help='Show this help message and exit.')
  parser.add_argument('-p','--prefix', metavar='<dir>', type=str, default=None, help='Specify the prefix directory.')
  args = vars(parser.parse_args())
  if 'help' in args:
    parser.print_help()
  else:
    ret = scr_prerun(prefix=args['prefix'])
    sys.exit(ret)

