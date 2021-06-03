#! /usr/bin/env python

# scr_common.py
# Common functions shared across scripts

import inspect
import scr_test_runtime

# for verbose, prints:
# filename:function:linenum -> event
# (filename ommitted if unavailable from frame)
# usage: sys.settrace(scr_common.tracefunction)
def tracefunction(frame,event,arg):
  try:
      print(inspect.getfile(frame).split('/')[-1]+':'+str(frame.f_code.co_name)+'():'+str(frame.f_lineno)+' -> '+str(event))
  except:
      print(str(frame.f_code.co_name)+'():'+str(frame.f_lineno)+' -> '+str(event))

def scr_test_runtime():
  return scr_test_runtime.scr_test_runtime()
