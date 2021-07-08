#! /usr/bin/env python3

# scr_list_dir.py
# This is the launcher script for list_dir.py

# This script returns info on the SCR control, cache, and prefix directories
# for the current user and jobid, it returns "INVALID" if something
# is not set.

# Better to have this directory construction in one place
# rather than having it duplicated over a number of different
# scripts

# returns 1 for error, string for success

import argparse, sys
from pyfe import scr_const
from pyfe.list_dir import list_dir
from pyfe.scr_environment import SCR_Env
from pyfe.scr_param import SCR_Param
from pyfe.resmgr import AutoResourceManager

if __name__=='__main__':
  parser = argparse.ArgumentParser(add_help=False, argument_default=argparse.SUPPRESS, prog='scr_list_dir')
  parser.add_argument('-h','--help', action='store_true', help='Show this help message and exit.')
  parser.add_argument('-u','--user', default=None, metavar='<user>', type=str, help='Specify username.')
  parser.add_argument('-j','--jobid', default=None, metavar='<id>', type=str, help='Specify jobid.')
  parser.add_argument('-b','--base', action='store_true', default=False, help='List base portion of cache/control directory')
  parser.add_argument('control/cache', choices=['control','cache'], metavar='<control | cache>', nargs='?', default=None, help='Specify the directory to list.')
  args = vars(parser.parse_args())
  if 'help' in args:
    parser.print_help()
    sys.exit(0)
  elif args['control/cache'] is None:
    print('Control or cache must be specified.')
    sys.exit(1)
  else:
    # TODO: read cache directory from config file
    bindir = scr_const.X_BINDIR
    # ensure scr_env is set
    scr_env = SCR_Env()
    scr_env.resmgr = AutoResourceManager()
    scr_env.param = SCR_Param()
    ret = list_dir(user=args['user'], jobid=args['jobid'], base=args['base'], runcmd=args['control/cache'], scr_env=scr_env, bindir=bindir)
    if type(ret) is int:
      sys.exit(ret)
    print(str(ret))
    sys.exit(0)

