#! /usr/bin/env python3

# scr_check_node.py

# check health of current node
#   control directory is available and of proper size
#   cache directory is available and of proper size
# print PASS if good, and FAIL if not

import argparse, os

def scr_check_node(free=False,cntl_list=None,cache_list=None):
  types = ['cntl','cache']
  # split up our lists of control directories / cache directories
  checkdict = {}
  for atype in types:
    if (atype == 'cntl' and cntl_list is not None) or (atype == 'cache' and cache_list is not None):
      checkdict[atype] = {}
      dirs = []
      if atype=='cntl':
        dirs = cntl_list.split(',')
      else:
        dirs = cache_list.split(',')
      for adir in dirs:
        if adir[-1] == '/':
          adir = adir[:-1]
        if ':' in adir:
          parts = adir.split(':')
          checkdict[atype][parts[0]] = {}
          checkdict[atype][parts[0]]['bytes'] = parts[1]
        else:
          checkdict[atype][adir] = {}
          checkdict[atype][adir]['bytes'] = None

  # check that we can access the directory
  for atype in checkdict:
    dirs = checkdict[atype].keys()
    # check that we can access the directory
    # (perl code ran an ls)
    # the docs suggest not to ask if access available, but to just try to access:
    # https://docs.python.org/3/library/os.html?highlight=os%20access#os.access

    # if a size is defined, check that the total size is enough
    for adir in dirs:
      try:
        if checkdict[atype][adir]['bytes'] is not None:
          # TODO: need to know which unit df is using
          # convert expected size from bytes to kilobytes
          kb = int(checkdict[atype][adir]['bytes']) // 1024

          # check total drive capacity, unless --free was given, then check free space on drive
          # ok, now get drive capacity
          statvfs = os.statvfs(adir)
          df_kb=0
          if free: # compare with usable free
            df_kb = statvfs.f_frsize * statvfs.f_bavail // 1024
          else: # compare with total
            df_kb = statvfs.f_frsize * statvfs.f_blocks // 1024
          if df_kb < kb:
            print('scr_check_node: FAIL: Insufficient space in directory: '+adir+', expected '+str(kb)+' KB, found '+str(df_kb)+' KB')
            return 1
      except Exception as e:
        print(e)
        print('scr_check_node: FAIL: Could not access directory: '+adir)
        return 1

    # attempt to write to directory
    for adir in dirs:
      testfile = adir+'/testfile.txt'
      try:
        with open(testfile,'w') as outfile:
          pass
      except PermissionError:
        print('scr_check_node: FAIL: Lack permission to write test file: '+testfile)
        return 1
      except Exception as e: # PermissionError or (other error)
        print(e)
        print('scr_check_node: FAIL: Could not touch test file: '+testfile)
        # return 1 # for some other error it may be ok ?
      try:
        os.remove(testfile)
      except PermissionError:
        print('scr_check_node: FAIL: Lack permission to rm test file: '+testfile)
        return 1
      except Exception as e:
        print(e)
        print('scr_check_node: FAIL: Could not rm test file: '+testfile)
        # return 1
  return 0

if __name__=='__main__':
  parser = argparse.ArgumentParser(add_help=False,argument_default=argparse.SUPPRESS,prog='scr_check_node',description='Checks that the current node is healthy')
  # default=None, required=True, nargs='+'
  parser.add_argument('-h', '--help', action='store_true', help='Show this help message and exit.')
  parser.add_argument('--free', action='store_true', default=False, help='Check that free capacity of drive meets limit, checks total capacity of drive otherwise.')
  parser.add_argument('--cntl', metavar='<dir>', type=str, default=None, help='Specify the SCR control directory.')
  parser.add_argument('--cache', metavar='<dir>', type=str, default=None, help='Specify the SCR cache directory.')
  args = vars(parser.parse_args())
  if 'help' in args:
    parser.print_help()
  else:
    ret = scr_check_node(free=args['free'],cntl_list=args['cntl'],cache_list=args['cache'])
    if ret==0:
      print('scr_check_node: PASS')

