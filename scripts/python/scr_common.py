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

