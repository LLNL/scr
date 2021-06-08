#! /usr/bin/env python

# scr_list_dir.py

# This script returns info on the SCR control, cache, and prefix directories
# for the current user and jobid, it returns "INVALID" if something
# is not set.

# Better to have this directory construction in one place
# rather than having it duplicated over a number of different
# scripts

import scr_const
from scr_param import SCR_Param
from scr_common import getconf

def print_usage(prog):
  print('')
  print('Usage:  '+prog+' [options] <control | cache>')
  print('')
  print('  Options:')
  print('    -u, --user     Specify username.')
  print('    -j, --jobid    Specify jobid.')
  print('    -b, --base     List base portion of cache/control directory')
  print('')

# returns 1 for error, 0 (or string) for success
def scr_list_dir(argv,scr_env=None):
  param = SCR_Param()
  # TODO: read cache directory from config file
  prog = "scr_list_dir"
  bindir = scr_const.X_BINDIR

  # read in command line arguments
  conf = getconf(argv,keyvals={'-u':'user','--user':'user','-j':'jobid','--jobid':'jobid','-b':'base','--base':'base','control':'runcmd','cache':'runcmd'})
  if conf is None:
    print_usage(prog)
    return 1

  # check that user specified "control" or "cache"
  if 'runcmd' not in conf:
    print_usage(prog)
    return 1

  runcmd = conf['runcmd']
  # get the base directory
  bases = []
  if runcmd=='cache':
    # lookup cache base
    cachedesc = param.get_hash('CACHE')
    if cachedesc is not None:
      #for i 
      #foreach my $index (keys %$cachedesc) {
      #  push @bases, $index;
      pass
    else:
      # lookup cntl base
      bases = param.get('SCR_CNTL_BASE')
  if len(bases)==0:
    print('INVALID')
    return 1

  # ensure scr_env is set
  if scr_env is None:
    scr_env = SCR_Env()
  # get the user/job directory
  suffix = ''
  if 'base' not in conf:
    # if not specified, read username from environment
    if 'user' not in conf:
      conf['user'] = scr_env.conf['user']
    # if not specified, read jobid from environment
    if 'jobid' not in conf:
      conf['jobid'] = scr_env.conf['jobid']
    # check that the required environment variables are set
    if conf['user'] is None or conf['jobid'] is None:
      # something is missing, print invalid dir and exit with error
      print('INVALID')
      return 1
    suffix = conf['user']+'/scr.'+conf['jobid']

  # ok, all values are here, print out the directory name and exit with success
  dirs = []
  for base in bases:
    if suffix!='':
      dirs.append(base+'/'+suffix)
    else:
      dirs.append(base)
  dirs = ' '.join(dirs)
  return dirs

