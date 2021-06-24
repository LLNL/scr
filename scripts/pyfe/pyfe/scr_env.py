#! /usr/bin/env python3

# scr_env.py
# SCR_Env contains general values from the environment

import os
from pyfe import scr_const
from pyfe.scr_common import scr_prefix

class SCR_Env:
  def __init__(self,env=None):
    # should this still be set here ?
    # it could be changed later... not certain this will be needed though
    #if env is None:
    #  env = scr_const.SCR_RESOURCE_MANAGER
    # we can keep a reference to the launcher and resource manager
    self.launcher = None
    self.resmgr = None
    # initialize the infos
    self.conf = {}
    #self.conf['env'] = env
    self.conf['prefix'] = scr_prefix()
    self.conf['nodes_file'] = scr_const.X_BINDIR+'/scr_nodes_file'
    self.conf['user'] = os.environ.get('USER')
    #self.conf['jobid'] = self.getjobid()
    #self.conf['nodes'] = self.get_job_nodes()

  # set the prefix
  def set_prefix(self,prefix):
    self.conf['prefix'] = prefix

if __name__ == '__main__':
  scr_env = SCR_Env()
  for attr in dir(scr_env):
    if attr.startswith('__'):
      continue
    thing = getattr(scr_env,attr)
    if type(thing) is dict:
      print('scr_env.'+attr+' = {}')
      for key in thing:
        print('scr_env.'+attr+'['+key+'] = '+str(thing[key]))
    else:
      print('scr_env.'+attr+' = '+str(thing))

