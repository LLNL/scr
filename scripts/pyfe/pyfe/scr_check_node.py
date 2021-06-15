#! /usr/bin/env python

# scr_check_node.py

# check health of current node
#   control directory is available and of proper size
#   cache directory is available and of proper size
# print PASS if good, and FAIL if not

import os, sys
from scr_common import getconf

def print_usage(prog):
  print('')
  print('  '+prog+' -- checks that the current node is healthy')
  print('')
  print('  Usage:  scr_check_node [--cntl <dir>] [--cache <dir>]')
  print('')
  print('  Options:')
  print('    --free             Check that free capacity of drive meets limit,')
  print('                         checks total capactiy of drive otherwise.')
  print('    --cntl <dir>       Specify the SCR control directory.')
  print('    --cache <dir>      Specify the SCR cache directory.')
  print('')

def scr_check_node(argv):
  prog = 'scr_check_node'
  conf = getconf(argv,{'--cntl':'cntl_list','--cache':'cache_list'},{'--free':'free'})
  if conf is None:
    print_usage(prog)
    return 0

  types = ['cntl','cache']
  # split up our lists of control directories / cache directories
  for atype in types:
    if (atype == 'cntl' and 'cntl_list' in conf) or (atype == 'cache' and 'cache_list' in conf):
      conf[atype] = {}
      dirs = []
      if atype=='cntl':
        dirs = conf['cntl_list'].split(',')
      else:
        dirs = conf['cache_list'].split(',')
      for adir in dirs:
        if ':' in adir:
          parts = adir.split(':')
          conf[atype][parts[0]] = {}
          conf[atype][parts[0]]['bytes'] = parts[1]
        else:
          conf[atype][adir] = {}
          conf[atype][adir]['bytes'] = None

  # check that we can access the directory
  for atype in types:
    if atype not in conf:
      continue
    dirs = conf[atype].keys()
    # check that we can access the directory
    # (perl code ran an ls)
    # the docs suggest not to ask if access available, but to just try to access:
    # https://docs.python.org/3/library/os.html?highlight=os%20access#os.access

    # if a size is defined, check that the total size is enough
    for adir in dirs:
      try:
        if conf[atype][adir]['bytes'] is not None:
          # TODO: need to know which unit df is using
          # convert expected size from bytes to kilobytes
          kb = int(conf[atype][adir]['bytes']) // 1024

          # check total drive capacity, unless --free was given, then check free space on drive
          df_arg_pos = 2
          if 'free' in conf:
            df_arg_pos = 4

          # ok, now get drive capacity
          statvfs = os.statvfs(adir)
          if 'free' in conf: # compare with usable free
            df_kb = statvfs.f_frsize * statvfs.f_bavail // 1024
            if df_kb < kb:
              print('FAIL: Insufficient space in directory: '+adir+', expected '+str(kb)+' KB, found '+str(df_kb)+' KB')
              return 1
          else: # compare with total
            df_kb = statvfs.f_frsize * statvfs.f_blocks // 1024
            if df_kb < kb:
              print('FAIL: Insufficient space in directory: '+adir+', expected '+str(kb)+' KB, found '+str(df_kb)+' KB')
              return 1
      except: # PermissionError:
        print('FAIL: Could not access directory: '+adir)
        return 1

    # attempt to write to directory
    for adir in dirs:
      testfile = adir+'/testfile.txt'
      try:
        with open(testfile,'w') as outfile:
          pass
      except: # PermissionError or (other error)
        #if sys.exc_info()[0] == PermissionError:
        print('FAIL: Could not touch test file: '+testfile)
        return 1
      try:
        os.remove(testfile)
      except:
        print('FAIL: Could not rm test file: '+testfile)
        return 1
  return 0

if __name__=='__main__':
  ret = scr_check_node(sys.argv[1:])
  print('scr_check_node returned '+str(ret))
