#! /usr/bin/env python

# scr_common.py
# Common functions shared across scripts

import inspect
from scr_list_dir import scr_list_dir as list_dir
from scr_prerun import scr_prerun as prerun
from scr_postrun import scr_postrun as postrun
from scr_test_runtime import scr_test_runtime as test_runtime
from scr_get_jobstep_id import scr_get_jobstep_id as get_jobstep_id
from scr_watchdog import scr_watchdog as watchdog
from scr_kill_jobstep import scr_kill_jobstep as kill_jobstep
from scr_list_down_nodes import scr_list_down_nodes as list_down_nodes
from scr_prefix import scr_prefix as prefix

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
  return test_runtime()

def scr_list_dir(args,scr_env):
  return list_dir(args,scr_env)  

def scr_prerun(args):
  return prerun(args)

def scr_postrun(argv):
  return postrun(argv)

def scr_get_jobstep_id(scr_env,srun_pid):
  return get_jobstep_id(scr_env,srun_pid)

def scr_watchdog(argv):
  return watchdog(argv)

def scr_kill_jobstep(*argv):
  return kill_jobstep(args)

def scr_list_down_nodes(argv,scr_env):
  return list_down_nodes(argv,src_env)

def scr_prefix():
  return prefix()
