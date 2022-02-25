#! /usr/bin/env python3

# scr_prerun.py

import os, sys

if 'scrjob' not in sys.path:
  sys.path.insert(0, '/'.join(os.path.realpath(__file__).split('/')[:-2]))
  import scrjob

import argparse
from datetime import datetime
from time import time

from scrjob.scr_test_runtime import SCR_Test_Runtime
from scrjob.scr_environment import SCR_Env
from scrjob.resmgrs import AutoResourceManager

def scr_prerun(scr_env=None):
  """this script is called after initialization to ensure verify the environment

  Prerun Operations
  -----------------
  Call SCR_Test_Runtime with a list of tests provided by the resmgr
  Ensures the .scr directory exists
  Remove existing flush or nodes files

  Returns
  -------
  int   0 - no error
        1 - error
  """
  # bail out if not enabled
  val = os.environ.get('SCR_ENABLE')
  if val == '0':
    return 0

  if scr_env is None or scr_env.resmgr is None:
    print('scr_prerun: ERROR: Unknown environment')
    return 1

  start_time = datetime.now()
  start_secs = int(time())
  print('scr_prerun: Started: ' + str(start_time))

  # check that we have all the runtime dependences we need
  if SCR_Test_Runtime(scr_env.resmgr.get_prerun_tests()) != 0:
    print('scr_prerun: exit code: 1')
    return 1

  # create the .scr subdirectory in the prefix directory
  dir_scr = scr_env.dir_scr()
  os.makedirs(dir_scr, exist_ok=True)

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
    os.remove(os.path.join(dir_scr, 'flush.scr'))
  except:  # error on doesn't exist / etc ...
    pass

  try:
    os.remove(os.path.join(dir_scr, 'nodes.scr'))
  except:
    pass

  # report timing info
  end_time = datetime.now()
  run_secs = int(time()) - start_secs
  print('scr_prerun: Ended: ' + str(end_time))
  print('scr_prerun: secs: ' + str(run_secs))

  # report exit code and exit
  print('scr_prerun: exit code: 0')
  return 0


if __name__ == '__main__':
  parser = argparse.ArgumentParser(add_help=False,
                                   argument_default=argparse.SUPPRESS,
                                   prog='scr_prerun')
  parser.add_argument('-h',
                      '--help',
                      action='store_true',
                      help='Show this help message and exit.')
  parser.add_argument('-p',
                      '--prefix',
                      metavar='<dir>',
                      type=str,
                      default=None,
                      help='Specify the prefix directory.')
  args = vars(parser.parse_args())

  if 'help' in args:
    parser.print_help()
  else:
    scr_env = SCR_Env(prefix=args['prefix'])
    scr_env.resmgr = AutoResourceManager()
    ret = scr_prerun(scr_env=scr_env)
    sys.exit(ret)
