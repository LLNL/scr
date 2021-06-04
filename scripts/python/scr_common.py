#! /usr/bin/env python

# scr_common.py
# Common functions shared across scripts

import inspect
import scr_list_dir, scr_prerun, scr_postrun, scr_test_runtime
import scr_get_jobstep_id, scr_watchdog, scr_kill_jobstep
import scr_list_down_nodes, scr_prefix

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

def scr_list_dir(args,scr_env):
  return scr_list_dir.scr_list_dir(args,scr_env)  

def scr_prerun(args):
  return scr_prerun.scr_prerun(args)

def scr_postrun(argv):
  return scr_postrun.scr_postrun(argv)

def scr_get_jobstep_id(scr_env,srun_pid):
  return scr_get_jobstep_id.scr_get_jobstep_id(scr_env,srun_pid)

def scr_watchdog(argv):
  return scr_watchdog.scr_watchdog(argv)

def scr_kill_jobstep(*argv):
  return scr_kill_jobstep.scr_kill_jobstep(args)

def scr_list_down_nodes(argv,scr_env):
  return scr_list_down_nodes.scr_list_down_nodes(argv,src_env)

def scr_prefix():
  return scr_prefix.scr_prefix()