#! /usr/bin/env python

# scr_common.py
# Defines for common functions shared across scripts

import inspect, os

# for verbose, prints:
# filename:function:linenum -> event
# (filename ommitted if unavailable from frame)
# usage: sys.settrace(scr_common.tracefunction)
def tracefunction(frame,event,arg):
  try:
      print(inspect.getfile(frame).split('/')[-1]+':'+str(frame.f_code.co_name)+'():'+str(frame.f_lineno)+' -> '+str(event))
  except:
      print(str(frame.f_code.co_name)+'():'+str(frame.f_lineno)+' -> '+str(event))

# Parses argv, returns a dictionary of the configuration options
# keyvals.keys is the key in argv to set keyvals[key] in the conf to argv option, (togglevals is for flag options to be set to True)
# if strict is True then it will return None if an invalid option is passed in
# if strict is False then any unused arguments will be appended to conf['argv'] = []
def getconf(argv,keyvals,togglevals=None,strict=True):
  conf = {}
  skip=False
  for i in range(len(argv)):
    if skip==True:
      skip=False
    elif '=' in argv[i]:
      vals = argv[i].split('=')
      if vals[0] in keyvals:
        conf[keyvals[vals[0]]] = vals[1]
      elif togglevals is not None and vals[0] in togglevals:
        conf[togglevals[vals[0]]] = vals[1]
      elif strict == True:
        return None
      else:
        if 'argv' not in conf:
          conf['argv'] = []
        conf['argv'].append(argv[i])
    else:
      if argv[i] in keyvals:
        if i+1==len(argv):
          return None
        conf[keyvals[argv[i]]]=argv[i+1]
        skip=True
      elif togglevals is not None and argv[i] in togglevals:
        conf[togglevals[argv[i]]]=1
      elif strict == True:
        return None
      else:
        if 'argv' not in conf:
          conf['argv'] = []
        conf['argv'].append(argv[i])
  return conf

# interpolate variables will expand environment variables in a string
# if the string begins with '~' or '.', these are first replaced by $HOME or $PWD
def interpolate_variables(varstr):
  # replace ~ and . symbols from front of path
  if varstr[0]=='~':
    varstr='$HOME'+varstr[1:]
  elif varstr[0]=='.':
    varstr=os.getcwd()+prefix[1:]
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

if __name__=='__main__':
  ret = scr_prefix()
  print('scr_prefix returned '+str(ret))

