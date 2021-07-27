#! /usr/bin/env python3

# list_down_nodes.py

from pyfe import scr_const, scr_common
from pyfe.list_dir import list_dir
from pyfe.scr_common import runproc, pipeproc


# mark any nodes specified on the command line
def remove_argument_excluded_nodes(nodes=[], nodeset_down=[]):
  #unavailable = {}
  for node in nodeset_down:
    if node in nodes:
      nodes.remove(node)
      #unavailable[node] = 'Specified on command line'
  #return unavailable


# The main scr_list_down_nodes method.
# this method takes an scr_env, the contained resource manager will determine which methods above to use
def list_down_nodes(reason=False,
                    free=False,
                    nodeset_down='',
                    runtime_secs=None,
                    nodeset=None,
                    scr_env=None,
                    log=None):
  if scr_env is None or scr_env.resmgr is None or scr_env.param is None:
    return 1
  bindir = scr_const.X_BINDIR

  # check that we have a nodeset before going any further
  resourcemgr = scr_env.resmgr
  if type(nodeset) is list:
    nodeset = ','.join(nodeset)
  elif nodeset is None:
    nodeset = resourcemgr.get_job_nodes()
  if nodeset is None or nodeset == '':
    print(
        'scr_list_down_nodes: ERROR: Nodeset must be specified or script must be run from within a job allocation.'
    )
    return 1

  param = scr_env.param

  # get list of nodes from nodeset
  nodes = scr_env.resmgr.expand_hosts(nodeset)

  ### In each of the scr_list_down_nodes.in
  ### these nodes are marked as unavailable, and also removed from the list to log
  ### There is no use to keep track of them in the unavailable dictionary
  #unavailable = list_argument_excluded_nodes(nodes=nodes,nodeset_down=nodeset_down)
  if nodeset_down != '':
    remove_argument_excluded_nodes(
        nodes=nodes, nodeset_down=scr_env.resmgr.expand_hosts(nodeset_down))

  # get strings here for the resmgr/nodetests.py
  # these are space separated strings with paths
  cntldir_string = list_dir(base=True,
                            runcmd='control',
                            scr_env=scr_env,
                            bindir=bindir)
  cachedir_string = list_dir(base=True,
                             runcmd='cache',
                             scr_env=scr_env,
                             bindir=bindir)

  # get a hash of all unavailable (down or excluded) nodes and reason
  # keys are nodes and the values are the reasons
  unavailable = resourcemgr.list_down_nodes_with_reason(
      nodes=nodes,
      scr_env=scr_env,
      free=free,
      cntldir_string=cntldir_string,
      cachedir_string=cachedir_string)

  # TODO: read exclude list from a file, as well?

  # print any failed nodes to stdout and exit with non-zero
  if len(unavailable) > 0:
    # log each newly failed node, along with the reason
    if log:
      for node in unavailable:
        note = node + ": " + unavailable[node]
        log.event('NODE_FAIL', note=note, secs=runtime_secs)

    # now output info to the user
    if reason:
      # list each node and the reason each is down
      reasons = []
      for node in unavailable:
        reasons.append(node + ': ' + unavailable[node])
      return "\n".join(reasons)
    else:
      # simply print the list of down node in range syntax
      # cast unavailable to a list to get only the keys of the dictionary
      return scr_env.resmgr.compress_hosts(list(unavailable))

  # otherwise, don't print anything and exit with 0
  return 0
