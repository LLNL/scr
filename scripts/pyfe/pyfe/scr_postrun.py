#! /usr/bin/env python3

# scr_postrun.py

# This script launches postrun.py
# Run this script after the final run in a job allocation
# to scavenge files from cache to parallel file system.

import os, sys

if 'pyfe' not in sys.path:
  sys.path.insert(0, '/'.join(os.path.realpath(__file__).split('/')[:-2]))
  import pyfe

import argparse
from pyfe.postrun import postrun
from pyfe.scr_environment import SCR_Env
from pyfe.scr_param import SCR_Param
from pyfe.resmgr import AutoResourceManager
from pyfe.joblauncher import AutoJobLauncher

if __name__ == '__main__':
  parser = argparse.ArgumentParser(add_help=False,
                                   argument_default=argparse.SUPPRESS,
                                   prog='scr_postrun')
  parser.add_argument('-h',
                      '--help',
                      action='store_true',
                      help='Show this help message and exit.')
  parser.add_argument('-j',
                      '--joblauncher',
                      type=str,
                      help='Required: Specify the job launcher.')
  parser.add_argument('-p',
                      '--prefix',
                      metavar='<dir>',
                      type=str,
                      default=None,
                      help='Specify the prefix directory.')
  parser.add_argument('-v',
                      '--verbose',
                      action='store_true',
                      default=False,
                      help='Verbose output.')
  args = vars(parser.parse_args())

  if 'help' in args or 'joblauncher' not in args:
    parser.print_help()
    sys.exit(0)
  else:
    scr_env = SCR_Env(prefix=args['prefix'])
    scr_env.resmgr = AutoResourceManager()
    scr_env.param = SCR_Param()
    scr_env.launcher = AutoJobLauncher(args['launcher'])
    ret = postrun(prefix_dir=args['prefix'],
                  scr_env=scr_env,
                  verbose=args['verbose'])
    sys.exit(ret)
