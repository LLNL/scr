#! /usr/bin/env python3

# scr_env.py
# SCR_Env contains general values from the environment

import argparse, os
from pyfe import scr_const
from pyfe.scr_common import scr_prefix
from pyfe.resmgr.scr_resourcemgr import SCR_Resourcemgr
from pyfe.scr_param import SCR_Param
from pyfe.joblauncher.scr_joblauncher import SCR_Joblauncher

class SCR_Env:
  def __init__(self):
    # we can keep a reference to the other objects
    self.param = None
    self.launcher = None
    self.resmgr = None
    # initialize the infos
    self.conf = {}
    self.conf['prefix'] = scr_prefix()
    self.conf['nodes_file'] = scr_const.X_BINDIR+'/scr_nodes_file'
    self.conf['user'] = os.environ.get('USER')
    self.conf['nodes'] = os.environ.get('SCR_NODELIST')

  # set the nodelist (called if the environment variable wasn't set)
  def set_nodelist(self,nodelist):
    self.conf['nodes'] = nodelist
    os.environ['SCR_NODELIST'] = nodelist

  # set the prefix
  def set_prefix(self,prefix):
    self.conf['prefix'] = prefix

  # list the number of nodes used in the last run
  def get_runnode_count(self):
    argv = [self.conf['nodes_file'], '--dir', self.conf['prefix']]
    out, returncode = runproc(argv=argv, getstdout=True)
    if returncode == 0:
      return int(out)
    return 0 # print(err)

def printobject(obj,objname):
  for attr in dir(obj):
    if attr.startswith('__'):
      continue
    thing = getattr(obj,attr)
    if thing is not None and (attr == 'resmgr' or attr == 'launcher' or attr == 'param'):
      printobject(thing,objname+'.'+attr)
    elif type(thing) is dict:
      print(objname+'.'+attr+' = {}')
      for key in thing:
        print(objname+'.'+attr+'['+key+'] = '+str(thing[key]))
    else:
      print(objname+'.'+attr+' = '+str(thing))

if __name__ == '__main__':
  parser = argparse.ArgumentParser(add_help=False, argument_default=argparse.SUPPRESS, prog='scr_env')
  parser.add_argument('-h','--help', action='store_true', help='Show this help message and exit.')
  parser.add_argument('-u','--user', action='store_true', help='List the username of current job.')
  parser.add_argument('-j','--jobid', action='store_true', help='List the job id of the current job.')
  parser.add_argument('-n','--nodes', action='store_true', help='List the nodeset the current job is using.')
  parser.add_argument('-d','--down', action='store_true', help='List any nodes of the job\'s nodeset that the resource manager knows to be down.')
  parser.add_argument('-p','--prefix', metavar='<dir>', type=str, help='Specify the prefix directory.')
  parser.add_argument('-r','--runnodes', action='store_true', help='List the number of nodes used in the last run.')
  args = vars(parser.parse_args())
  scr_env = SCR_Env()
  scr_env.resmgr = SCR_Resourcemgr()
  scr_env.launcher = SCR_Joblauncher()
  scr_env.param = SCR_Param()
  if len(args)==0:
    printobject(scr_env,'scr_env')
  elif 'help' in args:
    parser.print_help()
  else:
    scr_env.resmgr = SCR_Resourcemgr()
    if 'prefix' in args:
      scr_env.set_prefix(args['prefix'])
    if 'user' in args:
      print(str(scr_env.conf['user']),end='')
    if 'jobid' in args:
      print(str(scr_env.resmgr.conf['jobid']),end='')
    if 'nodes' in args:
      print(str(scr_env.resmgr.conf['nodes']),end='')
    if 'down' in args:
      print(str(scr_env.resmgr.get_downnodes()),end='')
    if 'runnodes' in args:
      print(str(scr_env.get_runnode_count()),end='')

