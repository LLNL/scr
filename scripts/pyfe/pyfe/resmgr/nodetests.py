#! /usr/bin/env python3

from pyfe import scr_const, scr_hostlist
from pyfe.scr_common import runproc, pipeproc

'''
 methods used by resource managers to test nodes
 these methods return a hash to track nodes which failed and their reason
 these methods take a list of nodes which would otherwise be used
 failing nodes are deleted from the list argument in each of these methods
'''
ping = 'ping'
bindir = scr_const.X_BINDIR
pdsh   = scr_const.PDSH_EXE
dshbak = scr_const.DSHBAK_EXE
# mark the set of nodes the resource manager thinks is down
def list_resmgr_down_nodes(nodes=[],resmgr_nodes=None):
  unavailable = {}
  if resmgr_nodes is not None:
    resmgr_nodes = scr_hostlist.expand(resmgr_nodes)
    for node in resmgr_nodes:
      if node in nodes:
        nodes.remove(node)
      unavailable[node] = 'Reported down by resource manager'
  return unavailable

# mark any nodes that fail to respond to (up to 2) ping(s)
def list_nodes_failed_ping(nodes=[]):
  unavailable = {}
  # `$ping -c 1 -w 1 $node 2>&1 || $ping -c 1 -w 1 $node 2>&1`;
  argv=[ping,'-c','1','-w','1','']
  for node in nodes:
    argv[5] = node
    returncode = runproc(argv=argv)[1]
    if returncode!=0:
      returncode = runproc(argv=argv)[1]
      if returncode!=0:
        unavailable[node] = 'Failed to ping'
  for node in unavailable:
    if node in nodes:
      nodes.remove(node)
  return unavailable

# mark any nodes to explicitly exclude via SCR_EXCLUDE_NODES
def list_param_excluded_nodes(nodes=[],param=None):
  unavailable = {}
  if param is not None:
    exclude = param.get('SCR_EXCLUDE_NODES')
    if exclude is not None:
      exclude_nodes = scr_hostlist.expand(exclude)
      for node in exclude_nodes:
        if node in nodes:
          nodes.remove(node)
          unavailable[node] = 'User excluded via SCR_EXCLUDE_NODES'
  return unavailable

# mark any nodes that don't respond to pdsh echo up
def list_pdsh_fail_echo(nodes=[]):
  unavailable = {}
  pdsh_assumed_down = nodes.copy()
  if len(nodes)>0:
    # only run this against set of nodes known to be responding
    upnodes = scr_hostlist.compress(nodes)

    # run an "echo UP" on each node to check whether it works
    argv = []
    argv.append([pdsh,'-f','256','-w',upnodes,'\"echo UP\"'])
    argv.append([dshbak,'-c'])
    output = pipeproc(argvs=argv,getstdout=True)[0]
    position=0
    for result in output.split('\n'):
      if len(result)==0:
        continue
      if position==0:
        if result.startswith('---'):
          position=1
      elif position==1:
        nodeset = result
        position = 2
      elif position==2:
        line=result
        position=3
      elif position==3:
        position=0
        if 'UP' in result:
          exclude_nodes = scr_hostlist.expand(nodeset)
          for excludenode in exclude_nodes:
            # this node responded, so remove it from the down list
            if excludenode in pdsh_assumed_down:
              pdsh_assumed_down.remove(excludenode)

  # if we still have any nodes assumed down, update our available/unavailable lists
  for node in pdsh_assumed_down:
    nodes.remove(node)
    unavailable[node] = 'Failed to pdsh echo UP'

  return unavailable

#### Each resource manager other than LSF had this section
#### Their difference was in the scr_check_node_argv
#### Only the SLURM had the line size = param.abtoull(size)
#### The abtoull will just return the int of the string if it isn't in the ab format
def check_dir_capacity(nodes=[], free=False, scr_env=None, scr_check_node_argv=[], cntldir_string=None, cachedir_string=None):
  if scr_check_node_argv == [] or nodes==[]:
    return {}
  if scr_env is None or scr_env.param is None:
    return {}
  unavailable = {}
  param = scr_env.param
  # specify whether to check total or free capacity in directories
  #if free: free_flag = '--free'

  # check that control and cache directories on each node work and are of proper size
  # get the control directory the job will use
  cntldir_vals = []
  # cntldir_string = `$bindir/scr_list_dir --base control`;
  if type(cntldir_string) is str and len(cntldir_string) != 0:
    dirs = cntldir_string.split(' ')
    cntldirs = param.get_hash('CNTLDIR')
    for base in dirs:
      if len(base)<1:
        continue
      val = base
      if cntldirs is not None and base in cntldirs and 'BYTES' in cntldirs[base]:
        if len(cntldirs[base]['BYTES'].keys())>0:
          size = list(cntldirs[base]['BYTES'].keys())[0] #(keys %{$$cntldirs{$base}{"BYTES"}})[0];
          #if (defined $size) {
          size = param.abtoull(size)
          #  $size = $param->abtoull($size);
          val += ':'+str(size)
          #  $val = "$base:$size";
      cntldir_vals.append(val)

  cntldir_flag = []
  if len(cntldir_vals)>0:
    cntldir_flag = ['--cntl ', ','.join(cntldir_vals)]

  # get the cache directory the job will use
  cachedir_vals = []
  #`$bindir/scr_list_dir --base cache`;
  if type(cachedir_string) is str and len(cachedir_string) != 0:
    dirs = cachedir_string.split(' ')
    cachedirs = param.get_hash('CACHEDIR')
    for base in dirs:
      if len(base)<1:
        continue
      val = base
      if cachedirs is not None and base in cachedirs and 'BYTES' in cachedirs[base]:
        if len(cachedirs[base]['BYTES'].keys())>0:
          size = list(cachedirs[base]['BYTES'].keys())[0]
          #my $size = (keys %{$$cachedirs{$base}{"BYTES"}})[0];
          #if (defined $size) {
          size = param.abtoull(size)
          #  $size = $param->abtoull($size);
          val += ':'+str(size)
          #  $val = "$base:$size";
      cachedir_vals.append(val)

  cachedir_flag = []
  if len(cachedir_vals) > 0:
    cachedir_flag = ['--cache ', ','.join(cachedir_vals)]

  # only run this against set of nodes known to be responding
  upnodes = scr_hostlist.compress(nodes)

  # run scr_check_node on each node specifying control and cache directories to check
  ################### This is calling a script, scr_check_node, from pdsh.
  ###################### think this is going to need the python pdsh equivalent
  #cray
  # [ $pdsh -Rexec -f 256 -w $upnodes aprun -n 1 -L %h ]
  #slurm [ $pdsh -Rexec -f 256 -w $upnodes srun -n 1 -N 1 -w %h ]
  #### the pmix should have from the scr_checknode to the end as a single argument (... ?)
  #pmix [ $pdsh -f 256 -w $upnodes ]
  # we need to replace the $vars with their actual values and complete the argv
  # full argv:
  # argv = []
  # argv.append( [ $pdsh $upnodes $scr_check_node $free_flag $cntldir_flag $cachedir_flag ] )
  # argv.append( [ $dshbak -c ] )
  # output = pipeproc(argv)
  argv = [ [] ]
  for arg in scr_check_node_argv:
    if arg=='$pdsh':
      argv[0].append(pdsh)
    elif arg=='$upnodes':
      argv[0].append(upnodes)
    else:
      argv[0].append(arg)
  argv[0].extend(['python3', bindir+'/scr_check_node.py'])
  if free:
    argv[0].append('--free')
  argv[0].extend(cntldir_flag)
  argv[0].extend(cachedir_flag)
  argv.append([dshbak,'-c'])
  output = pipeproc(argvs=argv,getstdout=True)[0]
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
          if node in nodes:
            nodes.remove(node)
            unavailable[node] = result
  return unavailable
