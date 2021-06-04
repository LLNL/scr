#! /usr/bin/env python

# scr_list_dir.py

# This script returns info on the SCR control, cache, and prefix directories
# for the current user and jobid, it returns "INVALID" if something
# is not set.

# Better to have this directory construction in one place
# rather than having it duplicated over a number of different
# scripts

from scr_param import SCR_Param

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
def scr_list_dir(args,scr_env=None):
  param = SCR_Param()
  # TODO: read cache directory from config file
  prog = "scr_list_dir"
  bindir = "@X_BINDIR@"

  # read in command line arguments
  conf = {}
  skip=False
  runcmd=''
  for i in range(len(args)):
    if skip==True:
      skip=False
    elif args[i]=='control' or args[i]=='cache':
      runcmd=args[i]
    elif '=' in args[i]:
      vals = args[i].split('=')
      if vals[0]=='--user' or vals[0]=='-u':
        conf['user']=vals[1]
      elif vals[0]=='--jobid' or vals[0]=='j':
        conf['jobid']=vals[1]
      elif vals[0]=='--base' or vals[0]=='b':
        conf['base']=vals[1]
    elif i<len(args)-1:
      if args[i]=='--user' or args[i]=='-u':
        conf['user']=args[i+1]
        skip=True
      elif args[i]=='--jobid' or args[i]=='-j':
        conf['jobid']=args[i+1]
        skip=True
      elif args[i]=='--base' or args[i]=='-b':
        conf['base']=args[i+1]
        skip=True

  # check that user specified "control" or "cache"
  if runcmd='':
    print_usage(prog)
    return 1

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

if __name__ == '__main__':
  scr_list_dir(['control'],SCR_Env('SLURM'))
