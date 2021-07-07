#! /usr/bin/env python3

# list_dir.py

# This script returns info on the SCR control, cache, and prefix directories
# for the current user and jobid, it returns "INVALID" if something
# is not set.

# Better to have this directory construction in one place
# rather than having it duplicated over a number of different
# scripts

def list_dir(user=None,jobid=None,base=False,runcmd=None,scr_env=None,bindir=''):
  # check that user specified "control" or "cache"
  if runcmd != 'control' and runcmd !='cache':
    return 1

  # TODO: read cache directory from config file
  bindir = scr_const.X_BINDIR

  # ensure scr_env is set
  if scr_env is None or scr_env.resmgr is None or scr_env.param is None:
    return 1

  # get the base directory
  bases = []
  if runcmd=='cache':
    # lookup cache base
    cachedesc = scr_env.param.get_hash('CACHE')
    if type(cachedesc) is dict:
      bases = list(cachedesc.keys())
      #foreach my $index (keys %$cachedesc) {
      #  push @bases, $index;
    elif cachedesc is not None:
      bases = [cachedesc]
    else:
      bases = []
  else:
    # lookup cntl base
    bases = scr_env.param.get('SCR_CNTL_BASE')
    if type(bases) is dict:
      bases = list(bases.keys())
    elif type(bases) is not None:
      bases = [bases]
    else:
      value = []
  if len(bases)==0:
    print('INVALID')
    return 1

  # get the user/job directory
  suffix = ''
  if base is False:
    # if not specified, read username from environment
    if user is None:
      user = scr_env.conf['user']
    # if not specified, read jobid from environment
    if jobid is None:
      jobid = scr_env.resmgr.conf['jobid']
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

