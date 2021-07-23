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
from pyfe.resmgr import AutoResourceManager

if __name__ == '__main__':
  parser = argparse.ArgumentParser(add_help=False,
                                   argument_default=argparse.SUPPRESS,
                                   prog='scr_postrun')
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
  parser.add_argument('-v',
                      '--verbose',
                      action='store_true',
                      default=False,
                      help='Verbose output.')
  args = vars(parser.parse_args())
  if 'help' in args:
    parser.print_help()
  else:
    scr_env = SCR_Env()
    scr_env.resmgr = AutoResourceManager()
    ret = postrun(prefix_dir=args['prefix'],
                  scr_env=scr_env,
                  verbose=args['verbose'])
    print(str(ret))
