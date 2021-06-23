#! /usr/bin/env python

# scr_list_down_nodes.py

import argparse
from time import time
from pyfe import scr_const, scr_hostlist
from pyfe.scr_param import SCR_Param
from pyfe.scr_list_dir import scr_list_dir
from pyfe.scr_common import runproc, pipeproc, scr_prefix
from pyfe import scr_common
from pyfe.env.scr_env import SCR_Env

def scr_list_down_nodes(reason=False, free=False, nodeset_down=None, log_nodes=False, runtime_secs=None, nodeset=None, scr_env=None):
  ping = 'ping'

  bindir = scr_const.X_BINDIR
  pdsh   = scr_const.PDSH_EXE
  dshbak = scr_const.DSHBAK_EXE

  start_time = str(int(time())) # epoch seconds as int to remove decimal, as string to be a parameter

  param = SCR_Param()

  # check that we have a nodeset before going any further
  if nodeset is not None:
    if type(nodeset) is not str:
      nodeset = ','.join(nodeset)
  else:
    nodeset=scr_env.conf['nodes']
  if nodeset is None or len(nodeset)<1:
    print('scr_list_down_nodes: ERROR: Nodeset must be specified or script must be run from within a job allocation.')
    return 1

  # get list of nodes from nodeset
  nodes = scr_hostlist.expand(nodeset)

  # get prefix directory
  prefix = scr_prefix()

  # get jobid
  if scr_env is None:
    scr_env = SCR_Env()
  jobid = scr_env.getjobid()

  # this hash defines all nodes available in our allocation
  allocation = {}
  available = {}
  for node in nodes:
    allocation[node] = True
    available[node] = True

  # hashes to define all unavailable (down or excluded) nodes and reason
  unavailable = {}
  reason = {}

  # mark the set of nodes the resource manager thinks is down
  resmgr_down = scr_env.get_downnodes()
  if resmgr_down is not None and resmgr_down!='':
    resmgr_nodes = scr_hostlist.expand(resmgr_nodes)
    for node in resmgr_nodes:
      if node in available:
        del available[node]
      unavailable[node] = True
      reason[node] = "Reported down by resource manager"

  # mark the set of nodes we can't ping
  failed_ping = {}
  for node in nodes:
    # ICMP over omnipath can fail due to bad arp
    # First ping will replenish arp, second succeed
    # `ping -c2` will be slower in the "normal" case
    # that ping succeeds, because non-root users cannot
    # set the ping interval below 0.2 seconds.
    argv=[ping,'-c','1','-w','1',node]
    returncode = runproc(argv=argv)[1]
    # `$ping -c 1 -w 1 $node 2>&1 || $ping -c 1 -w 1 $node 2>&1`;
    if returncode!=0:
      returncode = runproc(argv=argv)[1]
      if returncode!=0:
        if node in available:
          del available[node]
        unavailable[node] = True
        reason[node] = 'Failed to ping'

  # mark any nodes to explicitly exclude via SCR_EXCLUDE_NODES
  exclude = param.get('SCR_EXCLUDE_NODES')
  if exclude is not None:
    exclude_nodes = scr_hostlist.expand(exclude)
    for node in exclude_nodes:
      if node in allocation:
        if node in available:
          del available[node]
        unavailable[node] = True
        reason[node] = 'User excluded via SCR_EXCLUDE_NODES'

  # mark any nodes specified on the command line
  if nodeset_down is not None:
    exclude_nodes = scr_hostlist.expand(nodeset_down)
    for node in exclude_nodes:
      if node in allocation:
        if node in available:
          del available[node]
        unavailable[node] = True
        reason[node] = 'Specified on command line'

  # TODO: read exclude list from a file, as well?

  # specify whether to check total or free capacity in directories
  #if free: free_flag = '--free'

  # check that control and cache directories on each node work and are of proper size
  # get the control directory the job will use
  cntldir_vals = []
  cntldir_string = scr_list_dir(base=True,runcmd='control',scr_env=scr_env)
  # cntldir_string = `$bindir/scr_list_dir --base control`;
  if cntldir_string != 1:
    dirs = cntldir_string.split(' ')
    cntldirs = param.get_hash('CNTLDIR')
    for base in dirs:
      if len(base)<1:
        continue
      val = base
      if cntldirs is not None and base in cntldirs and 'BYTES' in cntldirs[base]:
        size = list(cntldirs[base]['BYTES'].keys())[0] #(keys %{$$cntldirs{$base}{"BYTES"}})[0];
        #if (defined $size) {
        if size is not None:
          size = param.abtoull(size)
          #  $size = $param->abtoull($size);
          val += ':'+str(size)
          #  $val = "$base:$size";
      cntldir_vals.append(val)

  cntldir_flag = ''
  if len(cntldir_vals)>0:
    cntldir_flag = '--cntl ' + ','.join(cntldir_vals)

  # get the cache directory the job will use
  cachedir_vals = []
  cachedir_string = scr_list_dir(base=True,runcmd='cache',scr_env=scr_env) #`$bindir/scr_list_dir --base cache`;
  if cachedir_string != 1:
    dirs = cachedir_string.split(' ')
    cachedirs = param.get_hash('CACHEDIR')
    for base in dirs:
      if len(base)<1:
        continue
      val = base
      if cachedirs is not None and base in cachedirs and 'BYTES' in cachedirs[base]:
        size = list(cachedirs[base]['BYTES'].keys())[0]
        #my $size = (keys %{$$cachedirs{$base}{"BYTES"}})[0];
        #if (defined $size) {
        if size is not None:
          size = param.abtoull(size)
          #  $size = $param->abtoull($size);
          val += ':'+str(size)
          #  $val = "$base:$size";
      cachedir_vals.append(val)

  cachedir_flag = ''
  if len(cachedir_vals) > 0:
    cachedir_flag = '--cache ' + ','.join(cachedir_vals)

  # only run this against set of nodes known to be responding
  still_up = list(available.keys())
  upnodes = scr_hostlist.compress(still_up)

  # run scr_check_node on each node specifying control and cache directories to check
  ################### This is calling a script, scr_check_node, from pdsh.
  ###################### think this is going to need the python pdsh equivalent
  if len(still_up) > 0:
    #argv = [pdsh,'-Rexec','-f','256','-w',upnodes,'srun','-n','1','-N','1','-w','%h',bindir+'/scr_check_node']
    argv = [pdsh,'-Rexec','-f','256','-w',upnodes,'srun','-n','1','-N','1','-w','%h','python3',bindir+'/scr_check_node.py']
    if free:
      argv.append('--free')
    argv.extend([cntldir_flag,cachedir_flag])
    argv = [ argv , [dshbak,'-c'] ]
    output = pipeproc(argvs=argv,getstdout=True)[0]
    #output = `$pdsh -Rexec -f 256 -w '$upnodes' srun -n 1 -N 1 -w %h $bindir/scr_check_node $free_flag $cntldir_flag $cachedir_flag | $dshbak -c`;
    action=0 # tracking action to use range iterator and follow original line <- shift flow
    nodeset = ''
    line = ''
    for result in output.split('\n'):
      if len(result)<1:
        pass
      elif action==0:
        if result.startswith('---'):
          action=1
      elif action==1:
        nodeset = result
        action=2
      elif action==2:
        line = result
        action=3
      elif action==3:
        action=0
        if 'PASS' not in result:
          exclude_nodes = scr_hostlist.expand(nodeset);
          for node in exclude_nodes:
            if node in allocation:
              if node in available:
                del available[node]
              unavailable[node] = True
              reason[node] = result

  # take union of all these sets
  failed_nodes = set(unavailable.keys())

  newly_failed_nodes = {}
  # print any failed nodes to stdout and exit with non-zero
  if len(failed_nodes)>0:
    # initialize our list of newly failed nodes to be all failed nodes
    for node in failed_nodes:
      newly_failed_nodes[node] = True

    # remove any nodes that user already knew to be down
    if nodeset_down is not None:
      exclude_nodes = scr_hostlist.expand(nodeset_down)
      for node in exclude_nodes:
        if node in newly_failed_nodes:
          del newly_failed_nodes[node]

    # log each newly failed node, along with the reason
    if log_nodes:
      for node in newly_failed_nodes.keys():
        duration = None
        if runtime_secs is not None:
          duration = runtime_secs
        scr_common.log(bindir=bindir,prefix=prefix,jobid=jobid,event_type='NODE_FAIL',event_note=node+': '+reason[node],event_start=start_time,event_secs=duration)
        #`$bindir/scr_log_event -i $jobid -p $prefix -T 'NODE_FAIL' -N '$node: $reason{$node}' -S $start_time $duration`;
    # now output info to the user
    ret=''
    if reason:
      # list each node and the reason each is down
      for node in failed_nodes:
        ret += node+': '+reason[node]+'\n'
      if len(ret)>1:
        ret = ret[:-1] # take off the trailing newline
    else:
      # simply print the list of down node in range syntax
      ret = scr_hostlist.compress(failed_nodes)
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
    if ret==0:
      print('No down nodes found.')
    else:
      print('scr_list_down_nodes returned '+str(ret))

