#! /usr/bin/env python3

# scr_list_dir.py

import os, sys

if 'scrjob' not in sys.path:
  sys.path.insert(0, '/'.join(os.path.realpath(__file__).split('/')[:-2]))
  import scrjob

import argparse
from scrjob import scr_const
from scrjob.list_dir import list_dir
from scrjob.scr_environment import SCR_Env
from scrjob.scr_param import SCR_Param
from scrjob.resmgrs import AutoResourceManager

if __name__ == '__main__':
  """this is an external driver for the internal list_dir method

  This script is for stand-alone purposes and is not used within other scripts

  This script can be used to test the list_dir method of list_dir.py
  """
  parser = argparse.ArgumentParser(add_help=False,
                                   argument_default=argparse.SUPPRESS,
                                   prog='scr_list_dir')
  parser.add_argument('-h',
                      '--help',
                      action='store_true',
                      help='Show this help message and exit.')
  parser.add_argument('-u',
                      '--user',
                      default=None,
                      metavar='<user>',
                      type=str,
                      help='Specify username.')
  parser.add_argument('-j',
                      '--jobid',
                      default=None,
                      metavar='<id>',
                      type=str,
                      help='Specify jobid.')
  parser.add_argument('-b',
                      '--base',
                      action='store_true',
                      default=False,
                      help='List base portion of cache/control directory')
  parser.add_argument('-p',
                      '--prefix',
                      default=None,
                      metavar='<id>',
                      type=str,
                      help='Specify the prefix directory.')
  parser.add_argument('control/cache',
                      choices=['control', 'cache'],
                      metavar='<control | cache>',
                      nargs='?',
                      default=None,
                      help='Specify the directory to list.')
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
    scr_env = SCR_Env(prefix=args['prefix'])
    scr_env.resmgr = AutoResourceManager()
    scr_env.param = SCR_Param()
    ret = list_dir(user=args['user'],
                   jobid=args['jobid'],
                   base=args['base'],
                   runcmd=args['control/cache'],
                   scr_env=scr_env,
                   bindir=bindir)
    if type(ret) is int:
      # an error message will already have been printed
      sys.exit(ret)
    # print the returned space separated string
    print(str(ret))
    sys.exit(0)
