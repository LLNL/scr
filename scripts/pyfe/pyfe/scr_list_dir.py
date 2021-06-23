#! /usr/bin/env python

# scr_list_dir.py

# This script returns info on the SCR control, cache, and prefix directories
# for the current user and jobid, it returns "INVALID" if something
# is not set.

# Better to have this directory construction in one place
# rather than having it duplicated over a number of different
# scripts

import argparse
from pyfe import scr_const
from pyfe.scr_param import SCR_Param

# returns 1 for error, 0 (or string) for success
def scr_list_dir(user=None,jobid=None,base=False,runcmd=None,scr_env=None):
  param = SCR_Param()
  # TODO: read cache directory from config file
  bindir = scr_const.X_BINDIR

  # check that user specified "control" or "cache"
  if runcmd is None:
    return 1

  # get the base directory
  bases = []
  if runcmd=='cache':
    # lookup cache base
    cachedesc = param.get_hash('CACHE')
    if cachedesc is not None:
      bases = list(cachedesc.keys())
      #foreach my $index (keys %$cachedesc) {
      #  push @bases, $index;
    else:
      # lookup cntl base
      bases = list(param.get('SCR_CNTL_BASE').keys())
  if len(bases)==0:
    print('INVALID')
    return 1

  # ensure scr_env is set
  if scr_env is None:
    scr_env = SCR_Env()
  # get the user/job directory
  suffix = ''
  if base is False:
    # if not specified, read username from environment
    if user is None:
      user = scr_env.conf['user']
    # if not specified, read jobid from environment
    if jobid is None:
      jobid = scr_env.conf['jobid']
    # check that the required environment variables are set
    if user is None or jobid is None:
      # something is missing, print invalid dir and exit with error
      print('INVALID')
      return 1
    suffix = user+'/scr.'+jobid

  # ok, all values are here, print out the directory name and exit with success
  dirs = []
  for abase in bases:
    if suffix!='':
      dirs.append(abase+'/'+suffix)
    else:
      dirs.append(abase)
  dirs = ' '.join(dirs)
  return dirs

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
  elif args['control/cache'] is None:
    print('Control or cache must be specified.')
  else:
    ret = scr_list_dir(user=args['user'],jobid=args['jobid'],base=args['base'],runcmd=args['control/cache'])
    if ret != 1:
      print(str(ret))

