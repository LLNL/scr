#! /usr/bin/env python3

# scr_common.py
# Defines for common functions shared across scripts

import argparse, inspect, os, sys
from pyfe import scr_const
from subprocess import Popen, PIPE

# for verbose, prints:
# filename:function:linenum -> event
# (filename ommitted if unavailable from frame)
# usage: sys.settrace(scr_common.tracefunction)
def tracefunction(frame,event,arg):
  try:
      print(inspect.getfile(frame).split('/')[-1]+':'+str(frame.f_code.co_name)+'():'+str(frame.f_lineno)+' -> '+str(event))
  except:
      print(str(frame.f_code.co_name)+'():'+str(frame.f_lineno)+' -> '+str(event))

# interpolate variables will expand environment variables in a string
# if the string begins with '~' or '.', these are first replaced by $HOME or $PWD
def interpolate_variables(varstr):
  if varstr is None or len(varstr)==0:
    return ''
  # replace ~ and . symbols from front of path
  if varstr[0]=='~':
    varstr='$HOME'+varstr[1:]
  elif varstr.startswith('..'):
    topdir = '/'.join(os.getcwd().split('/')[:-1])
    varstr = varstr[3:]
    while varstr.startswith('..'):
      topdir = '/'.join(topdir.split('/')[:-1])
      varstr = varstr[3:]
    varstr = topdir+'/'+varstr
  elif varstr[0]=='.':
    varstr=os.getcwd()+prefix[1:]
  if varstr[-1] == '/' and len(varstr) > 1:
    varstr = varstr[:-1]
  return os.path.expandvars(varstr)

# method to return the scr prefix (as originally in scr_param.pm.in)
def scr_prefix():
  prefix = os.environ.get('SCR_PREFIX')
  if prefix is None:
    return os.getcwd()
  # tack on current working dir if needed
  # don't resolve symlinks
  # don't worry about missing parts, the calling script calling might create it
  return interpolate_variables(prefix)

#####
'''
The default shell used by subprocess is /bin/sh. If you’re using other shells, like tch or csh, you can define them in the executable argument.
'''
###

# calls subprocessPopen using argv for program+arguments
# return value is always a pair, [0] is output or None, [1] is returncode or pid
# specify wait=False to return the pid (don't wait for a returncode / output)
# with wait=False the first return value is the Popen and the second is the pid
# to return the pid requires the shell argument of subprocess.Popen to be false (default)
# for the first return value (output) -> specify getstdout to get the stdout, getstderr to get stderr
# specifying both getstdout and getstderr=True returns a list where [0] is stdout and [1] is stderr
def runproc(argv,wait=True,getstdout=False,getstderr=False):
  if len(argv)<1:
    return None, None
  try:
    runproc = Popen(argv, bufsize=1, stdin=None, stdout=PIPE, stderr=PIPE, universal_newlines=True)
    if wait==False:
      return runproc, runproc.pid
    if getstdout==True and getstderr==True:
      output = runproc.communicate()
      return output, runproc.returncode
    if getstdout==True:
      output = runproc.communicate()[0]
      return output, runproc.returncode
    if getstderr==True:
      output = runproc.communicate()[1]
      return output, runproc.returncode
    runproc.communicate()
    return None, runproc.returncode
  except Exception as e:
    print('runproc: ERROR: '+str(e))
    return None, None

# pipeproc works as runproc above, except argvs is a list of argv lists
# the first subprocess is opened and from there stdout is chained to stdin
# values returned (returncode/pid/stdout/stderr) will be from the final process
def pipeproc(argvs,wait=True,getstdout=False,getstderr=False):
  if len(argvs)<1:
    return None, None
  if len(argvs)==1:
    return runproc(argvs[0],wait,getstdout,getstderr)
  try:
    nextprog = Popen(argvs[0], bufsize=1, stdin=None, stdout=PIPE, stderr=PIPE, universal_newlines=True)
    for i in range(1,len(argvs)):
      pipeprog = Popen(argvs[i], stdin=nextprog.stdout, stdout=PIPE, stderr=PIPE, bufsize=1, universal_newlines=True)
      nextprog.stdout.close()
      nextprog = pipeprog
    if wait==False:
      return nextprog, nextprog.pid
    if getstdout==True and getstderr==True:
      output = nextprog.communicate()
      return output, nextprog.returncode
    if getstdout==True:
      output = nextprog.communicate()[0]
      return output, nextprog.returncode
    if getstderr==True:
      output = nextprog.communicate()[1]
      return output, nextprog.returncode
    return None, nextprog.returncode
  except Exception as e:
    print('pipeproc: ERROR: '+str(e))
    return None, None

# passes the given arguments to bindir/scr_log_event
# prefix is required
# This was called from scr_list_down_nodes in a loop, with event_note changing
# To handle this, event_note can either be a string or a dictionary
# if event_note is a dictionary the looped runproc will happen here
def log(bindir=None, prefix=None, username=None, jobname=None, jobid=None, start=None, event_type=None, event_note=None, event_dset=None, event_name=None, event_start=None, event_secs=None):
  if prefix is None:
    prefix = scr_prefix()
    #print('log: prefix is required')
  if bindir is None:
    bindir = scr_const.X_BINDIR
  argv = [bindir+'/scr_log_event','-p',prefix]
  if username is not None:
    argv.extend(['-u',username])
  if jobname is not None:
    argv.extend(['-j',jobname])
  if jobid is not None:
    argv.extend(['-i',jobid])
  if start is not None:
    argv.extend(['-s',start])
  if event_type is not None:
    argv.extend(['-T',event_type])
  if event_dset is not None:
    argv.extend(['-D',event_dset])
  if event_name is not None:
    argv.extend(['-n',event_name])
  if event_start is not None:
    argv.extend(['-S',event_start])
  if event_secs is not None:
    argv.extend(['-L',event_secs])
  if type(event_note) is dict:
    argv.extend(['-N',''])
    lastarg = len(argv)-1
    for key in event_note:
      argv[lastarg] = key+':'+event_note[key]
      runproc(argv=argv)
  else:
    if event_note is not None:
      argv.extend(['-N',event_note])
    runproc(argv=argv)

if __name__=='__main__':
  parser = argparse.ArgumentParser(add_help=False, argument_default=argparse.SUPPRESS, prog='scr_common')
  parser.add_argument('-h', '--help', action='store_true', help='Show this help message and exit.')
  parser.add_argument('--interpolate', metavar='<variable>', type=str, help='Interpolate a variable string.')
  parser.add_argument('--prefix', action='store_true', help='Print the SCR prefix.')
  parser.add_argument('--runproc', nargs=argparse.REMAINDER, help='Launch process with arguments')
  parser.add_argument('--pipeproc', nargs=argparse.REMAINDER, help='Launch processes and pipe output to other processes. (separate processes with a colon)')
  parser.add_argument('--log', nargs='+', metavar='<option=value>', help='Create a log entry, available options: bindir, prefix, username, jobname, jobid, start, event_type, event_note, event_dset, event_name, event_start, event_secs')
  args = vars(parser.parse_args())
  if 'help' in args:
    parser.print_help()
    sys.exit(1)
  if 'interpolate' in args:
    print('interpolate_variables('+args['interpolate']+')')
    print('  -> '+str(interpolate_variables(args['interpolate'])))
  if 'prefix' in args:
    print('scr_prefix()')
    print('  -> '+str(scr_prefix()))
  if 'runproc' in args:
    print('runproc('+' '.join(args['runproc'])+')')
    out, returncode = runproc(argv=args['runproc'],getstdout=True,getstderr=True)
    print('  process returned with code '+str(returncode))
    print('  stdout:')
    print(out[0])
    print('  stderr:')
    print(out[1])
  if 'pipeproc' in args:
    printstr = 'pipeproc( '
    argvs = []
    argvs.append([])
    i=0
    for arg in args['pipeproc']:
      if arg==':':
        i+=1
        argvs.append([])
        printstr+='| '
      else:
        argvs[i].append(arg)
        printstr+=arg+' '
    print(printstr+')')
    out, returncode = pipeproc(argvs=argvs,getstdout=True,getstderr=True)
    print('  final process returned with code '+str(returncode))
    if out is not None:
      print('  stdout:')
      print(out[0])
      print('  stderr:')
      print(out[1])
  if 'log' in args:
    bindir,prefix,username,jobname = None,None,None,None
    jobid,start,event_type,event_note = None,None,None,None
    event_dset,event_name,event_start,event_secs = None,None,None,None
    printstr='log('
    for keyvalpair in args['log']:
      if '=' not in keyvalpair:
        continue
      vals = keyvalpair.split('=')
      if vals[0]=='bindir':
        bindir=vals[1]
      elif vals[0]=='prefix':
        prefix=vals[1]
      elif vals[0]=='username':
        username=vals[1]
      elif vals[0]=='jobname':
        jobname=vals[1]
      elif vals[0]=='jobid':
        jobid=vals[1]
      elif vals[0]=='start':
        start=vals[1]
      elif vals[0]=='event_type':
        event_type=vals[1]
      elif vals[0]=='event_note':
        event_note=vals[1]
      elif vals[0]=='event_dset':
        event_dset=vals[1]
      elif vals[0]=='event_name':
        event_name=vals[1]
      elif vals[0]=='event_start':
        event_start=vals[1]
      elif vals[0]=='event_secs':
        event_secs=vals[1]
      else:
        continue
      printstr+=keyvalpair+', '
    if printstr[-1]=='(':
      print('Nothing to log, see \'--help\' for available value pairs.')
      sys.exit(1)
    print(printstr[:-2]+')')
    log(bindir=bindir, prefix=prefix, username=username, jobname=jobname, jobid=jobid, start=start, event_type=event_type, event_note=event_note, event_dset=event_dset, event_name=event_name, event_start=event_start, event_secs=event_secs)

