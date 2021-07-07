#! /usr/bin/env python3

# list_down_nodes.py

from time import time
from pyfe import scr_const, scr_hostlist, list_dir
from pyfe.scr_common import runproc, pipeproc, scr_prefix
from pyfe import scr_common

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
  if scr_env is None or scr_env.resmgr is None or scr_env.param is None:
    return 1
  bindir = scr_const.X_BINDIR
  pdsh   = scr_const.PDSH_EXE

  start_time = str(int(time())) # epoch seconds as int to remove decimal, as string to be a parameter

  # check that we have a nodeset before going any further
  resourcemgr = scr_env.resmgr
  if nodeset is None or len(nodeset)==0:
    nodeset = resourcemgr.conf['nodes']
    if nodeset is None or len(nodeset)==0:
      print('scr_list_down_nodes: ERROR: Nodeset must be specified or script must be run from within a job allocation.')
      return 1
  if type(nodeset) is not str:
    nodeset = ','.join(nodeset)

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
  cntldir_string = list_dir(base=True,runcmd='control',scr_env=scr_env,bindir=bindir)
  cachedir_string = list_dir(base=True,runcmd='cache',scr_env=scr_env,bindir=bindir)

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
