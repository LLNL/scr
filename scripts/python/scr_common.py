#! /usr/bin/env python

# scr_common.py
# Defines for common functions shared across scripts

import inspect

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
# keyvals.keys is the key in argv to set keyvals[key] in the conf to argv option, (togglevals are for flag options set to True)
# if strict is True then it will return None if an invalid option is passed in
def getconf(argv,keyvals,togglevals=None,strict=True):
  conf = {}
  skip=False
  for i in range(len(argv)):
    if skip==True:
      skip=False
    elif '=' in argv[i]:
      vals = argv[i].split('=')
      for key in keyvals:
        if vals[0]==key:
          conf[vals[0]] = vals[1]
          break
      if vals[0] not in conf and togglevals is not None:
        for key in togglevals:
          if key==vals[0]:
            conf[vals[0]] = vals[1]
            break
      if strict == True and vals[0] not in conf:
        return None
    else:
      for key in keyvals:
        if key==argv[i]:
          if i+1==len(argv):
            return None
          conf[argv[i]]=argv[i+1]
          skip=True
          break
      if skip==False:
        if togglevals is not None:
          for key in togglevals:
            if key==argv[i]:
              conf[argv[i]]=1
              break
        if strict == True and argv[i] not in conf:
          return None
  return conf
  
