#! /usr/bin/env python3

# scr_list_down_nodes.py

import argparse
from time import time
from pyfe import scr_const, scr_hostlist
from pyfe.scr_param import SCR_Param
from pyfe.scr_list_dir import scr_list_dir
from pyfe.scr_common import runproc, pipeproc, scr_prefix
from pyfe import scr_common
from pyfe.scr_env import SCR_Env
from pyfe.resmgr import AutoResourceManager

# mark any nodes specified on the command line
def remove_argument_excluded_nodes(nodes=[],nodeset_down=None):
  if nodeset_down is None:
    return
  #unavailable = {}
  exclude_nodes = scr_hostlist.expand(nodeset_down)
  for node in exclude_nodes:
    if node in nodes:
      nodes.remove(node)
      #unavailable[node] = 'Specified on command line'
  #return unavailable

# The main scr_list_down_nodes method.
# this method takes an scr_env, the contained resource manager will determine which methods above to use
def scr_list_down_nodes(reason=False, free=False, nodeset_down='', log_nodes=False, runtime_secs=None, nodeset=None, scr_env=None):
  bindir = scr_const.X_BINDIR
  pdsh   = scr_const.PDSH_EXE

  start_time = str(int(time())) # epoch seconds as int to remove decimal, as string to be a parameter

  # check that we have a nodeset before going any further
  if scr_env is None:
    scr_env = SCR_Env()
  if scr_env.resmgr is None:
    scr_env.resmgr = AutoResourceManager()
  resourcemgr = scr_env.resmgr
  if nodeset is None or len(nodeset)==0:
    nodeset = resourcemgr.conf['nodes']
    if nodeset is None or len(nodeset)==0:
      print('scr_list_down_nodes: ERROR: Nodeset must be specified or script must be run from within a job allocation.')
      return 1
  if type(nodeset) is not str:
    nodeset = ','.join(nodeset)

  if scr_env.param is None:
    scr_env.param = SCR_Param()
  param = scr_env.param

  # get list of nodes from nodeset
  nodes = scr_hostlist.expand(nodeset)

  # get prefix directory
  prefix = scr_env.conf['prefix']

  # get jobid
  jobid = resourcemgr.conf['jobid']
  #if jobid == 'defjobid': # job id could not be determined
  #  print('Could not determine the job id') # the only place this is used here is in the logging below

  ### In each of the scr_list_down_nodes.in
  ### these nodes are marked as unavailable, and also removed from the list to log
  ### There is no use to keep track of them in the unavailable dictionary
  #unavailable = list_argument_excluded_nodes(nodes=nodes,nodeset_down=nodeset_down)
  remove_argument_excluded_nodes(nodes=nodes,nodeset_down=nodeset_down)

  # get strings here for the resmgr/nodetests.py
  cntldir_string = scr_list_dir(base=True,runcmd='control',scr_env=scr_env)
  cachedir_string = scr_list_dir(base=True,runcmd='cache',scr_env=scr_env)

  # get a hash of all unavailable (down or excluded) nodes and reason
  # keys are nodes and the values are the reasons
  unavailable = resourcemgr.list_down_nodes_with_reason(nodes=nodes, scr_env=scr_env, free=free, cntldir_string=cntldir_string, cachedir_string=cachedir_string)

  # TODO: read exclude list from a file, as well?

  # print any failed nodes to stdout and exit with non-zero
  if len(unavailable)>0:
    # log each newly failed node, along with the reason
    if log_nodes:
      # scr_common.log calls the external program: scr_log_event
      # the method will also accept a dictionary (instead of a string)
      # for the event_note argument, this moves the loop closer to the runproc call
      scr_common.log(bindir=bindir, prefix=prefix, jobid=jobid, event_type='NODE_FAIL', event_note=unavailable, event_start=start_time, event_secs=runtime_secs)
    # now output info to the user
    ret=''
    if reason:
      # list each node and the reason each is down
      for node in unavailable:
        ret += node+': '+unavailable[node]+'\n'
      ret = ret[:-1] ### take off the final trailing newline (?)
    else:
      # simply print the list of down node in range syntax
      ret = scr_hostlist.compress(list(unavailable))
    return ret
  # otherwise, don't print anything and exit with 0
  return 0

if __name__=='__main__':
  parser = argparse.ArgumentParser(add_help=False, argument_default=argparse.SUPPRESS, prog='scr_list_down_nodes')
  parser.add_argument('--help', action='store_true', help='Show this help message and exit.')
  parser.add_argument('-r','--reason', action='store_true', default=False, help='Print reason node is down.')
  parser.add_argument('-f','--free', action='store_true', default=False, help='Test required drive space based on free amount, rather than capacity.')
  parser.add_argument('-d','--down', metavar='<nodeset>', type=str, default=None, help='Force nodes to be down without testing.')
  parser.add_argument('-l','--log', action='store_true', default=False, help='Add entry to SCR log for each down node.')
  parser.add_argument('-s','--secs', metavar='N', type=str, default=None, help='Specify the job\'s runtime seconds for SCR log.')
  parser.add_argument('[nodeset]', nargs='*', default=None, help='Specify the complete set of nodes to check within.')
  args = vars(parser.parse_args())
  if 'help' in args:
    parser.print_help()
  else:
    ret = scr_list_down_nodes(reason=args['reason'], free=args['free'], nodeset_down=args['down'], log_nodes=args['log'], runtime_secs=args['secs'], nodeset=args['[nodeset]'])
    print(str(ret),end='')

