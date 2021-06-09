#! /usr/bin/env python

# scr_list_down_nodes.py

import scr_const
from datetime import datetime
from scr_param import SCR_Param
from scr_list_dir import scr_list_dir
import scr_hostlist

def print_usage(prog):
  print('  '+prog+' -- tests and lists nodes that are not available for SCR')
  print('')
  print('  Usage:  '+prog+' [options] [nodeset]')
  print('')
  print('  Options:')
  print('    -r, --reason')
  print('          Print reason node is down')
  print('    -f, --free')
  print('          Test required drive space based on free amount, rather than capacity')
  print('    -d, --down=NODESET')
  print('          Force nodes to be down without testing')
  print('')
  print('    -l, --log')
  print('          Add entry to SCR log for each down node')
  print('    -s, --secs=N')
  print('          Specify the job\'s runtime seconds for SCR log')

def scr_list_down_nodes(argv,scr_env=None):
  prog = 'scr_list_down_nodes'
  ping = 'ping'

  bindir = scr_const.X_BINDIR
  pdsh   = scr_const.PDSH_EXE
  dshbak = scr_const.DSHBAK_EXE

  start_time = datetime.now()

  param = SCR_Param()

  conf = {}

  skip=False
  nodeset = ''
  for i in range(len(argv)):
    if skip==False:
      skip=True
    elif argv[i]=='--reason' or argv[i]=='-r':
      conf['reason']=True
    elif argv[i]=='--free' or argv[i]=='-f':
      conf['free']=True
    elif argv[i]=='--down' or argv[i]=='-d':
      if i+1==len(argv):
        print_usage(prog)
        return 1
      conf['down']=argv[i+1]
      skip=True
    elif argv[i]=='--log' or argv[i]=='-l':
      conf['log_nodes']=True
    elif argv[i]=='--secs' or argv[i]=='-s':
      if i+1==len(argv):
        print_usage(prog)
        return 1
      conf['runtime_secs']=argv[i+1]
    elif '=' in argv[i]:
      vals = argv[i].split('=')
      if vals[0]=='--down' or vals[0]=='-d':
        conf['down']=vals[1]
      elif vals[0]=='--secs' or vals[0]=='-s':
        conf['runtime_secs']=vals[1]
      else:
        print_usage(prog)
        return 1
    elif argv[i]=='--help' or argv[i]=='-h':
      print_usage(prog)
      return 1
    elif nodeset=='':
      nodeset=argv[i]

  # get prefix directory
  prefix = bindir+'/scr_prefix'

  # get jobid
  if scr_env is None:
    scr_env = SCR_Env()
  jobid = scr_env.getjobid()

  # check that we have a nodeset before going any further
  if nodeset=='':
    nodeset=scr_env.conf['nodes']
    if nodeset is None:
      print(prog+': ERROR: Nodeset must be specified or script must be run from within a job allocation.')
      return 1

  # get list of nodes from nodeset
  nodes = scr_hostlist.expand(nodeset)

  # this hash defines all nodes available in our allocation
  allocation = {}
  available = {}
  for node in nodes:
    allocation[node] = 1
    available[node] = 1

  # hashes to define all unavailable (down or excluded) nodes and reason
  unavailable = {}
  reason = {}

  # mark the set of nodes the resource manager thinks is down
  scr_env.set_downnodes() # <- attempt to set down nodes
  resmgr_down = '' if 'down' not in src_env.conf else scr_env.conf['down']
  if resmgr_down!='':
    resmgr_nodes = scr_hostlist.expand(resmgr_nodes)
    for node in resmgr_nodes:
      if node in available:
        del available[node]
      unavailable[node] = 1
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
    runproc = subprocess.Popen(args=argv, bufsize=1, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
    out = runproc.communicate()
    # `$ping -c 1 -w 1 $node 2>&1 || $ping -c 1 -w 1 $node 2>&1`;
    if runproc.returncode!=0:
      runproc = subprocess.Popen(args=argv, bufsize=1, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
      out = runproc.communicate()
      if runproc.returncode!=0:
        if node in available:
          del available[node]
        unavailable[node] = 1
        reason[node] = 'Failed to ping'

  # mark any nodes to explicitly exclude via SCR_EXCLUDE_NODES
  exclude = param.get('SCR_EXCLUDE_NODES')
  if exclude is not None:
    exclude_nodes = scr_hostlist.expand(exclude)
    for node in exclude_nodes:
      if node in allocation:
        if node in available:
          del available[node]
        unavailable[node] = 1
        reason[node] = 'User excluded via SCR_EXCLUDE_NODES'

  # mark any nodes specified on the command line
  if 'nodeset_down' in conf:
    exclude_nodes = scr_hostlist.expand(conf['nodeset_down'])
    for node in exclude_nodes:
      if node in allocation:
        if node in available:
          del available[node]
        unavailable[node] = 1
        reason[node] = 'Specified on command line'

  # TODO: read exclude list from a file, as well?

  # specify whether to check total or free capacity in directories
  free_flag = '--free' if 'free' in conf else ''

  # check that control and cache directories on each node work and are of proper size
  # get the control directory the job will use
  cntldir_vals = []
  cntldir_string = scr_list_dir('--base control',src_env)
  # cntldir_string = `$bindir/scr_list_dir --base control`;
  if type(cntldir_string) is str:
    dirs = cntldir_string.split(' ')
    cntldirs = param.get_hash('CNTLDIR')
    for base in dirs:
      val = base
      if cntldirs is not None and base in cntldirs and 'BYTES' in cntldirs[base]:
        size = cntldirs[base]['BYTES'].keys()[0] #(keys %{$$cntldirs{$base}{"BYTES"}})[0];
        #if (defined $size) {
        #  $size = $param->abtoull($size);
        #  $val = "$base:$size";
      cntldir_vals.append(val)

  cntldir_flag = ''
  if len(cntldir_vals)>0:
    pass
    #cntldir_flag = "--cntl " . join(",", @cntldir_vals);

  # get the cache directory the job will use
  cachedir_vals = []
  cachedir_string = scr_list_dir('--base cache',src_env) #`$bindir/scr_list_dir --base cache`;
  if type(cachedir_string) is str:
    dirs = cachedir_string.split(' ')
    cachedirs = param.get_hash('CACHEDIR')
    for base in dirs:
      val = base
      if cachedirs is not None and base in cachedirs and 'BYTES' in cachedirs[base]:
        size = cachedirs[base]['BYTES'].keys()[0]
        #my $size = (keys %{$$cachedirs{$base}{"BYTES"}})[0];
        #if (defined $size) {
        #  $size = $param->abtoull($size);
        #  $val = "$base:$size";
      cachedir_vals.append(val)

  cachedir_flag = ''
  if len(cachedir_vals) > 0:
    pass
    #cachedir_flag = "--cache " . join(",", @cachedir_vals);

  # only run this against set of nodes known to be responding
  still_up = available.keys()
  upnodes = scr_hostlist.compress(still_up)

  # run scr_check_node on each node specifying control and cache directories to check
  if len(still_up) > 0:
    argv=[pdsh,'-Rexec','-f','256','-w',upnodes,'srun','-n','1','-N','1','-w','%h',bindir+'/scr_check_node',free_flag,cntldir_flag,cachedir_flag]
    runproc = subprocess.Popen(args=argv, bufsize=1, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
    argv=[dshbak,'-c']
    runproc2 = subprocess.Popen(args=argv, bufsize=1, stdin=runproc, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
    output = runproc2.communicate()
    #output = `$pdsh -Rexec -f 256 -w '$upnodes' srun -n 1 -N 1 -w %h $bindir/scr_check_node $free_flag $cntldir_flag $cachedir_flag | $dshbak -c`;
    action=0 # tracking action to use range iterator and follow original line <- shift flow
    nodeset = ''
    line = ''
    for result in lines:
      if action==0:
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
              unavailable[node] = 1
              reason[node] = result

  # take union of all these sets
  failed_nodes = set(unavailable.keys())

  newly_failed_nodes = {}
  # print any failed nodes to stdout and exit with non-zero
  if len(failed_nodes)>0:
    # initialize our list of newly failed nodes to be all failed nodes
    for node in failed_nodes:
      newly_failed_nodes[node] = 1

    # remove any nodes that user already knew to be down
    if 'nodeset_down' in conf:
      exclude_nodes = scr_hostlist.expand(conf['nodeset_down'])
      for node in exclude_nodes:
        if node in newly_failed_nodes:
          del newly_failed_nodes[node]

    # log each newly failed node, along with the reason
    if 'log_nodes' in conf:
      for node in newly_failed_nodes.keys():
        duration = ''
        if 'runtime_secs' in conf:
          duration = '-D '+conf['runtime_secs']
        argv=[bindir+'/scr_log_event','-i',jobid,'-p',prefix,'-T','NODE_FAIL','-N',node+': '+reason[node],'-S',start_time,duration]
        runproc = subprocess.Popen(args=argv, bufsize=1, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
        runproc.communicate()
        #`$bindir/scr_log_event -i $jobid -p $prefix -T 'NODE_FAIL' -N '$node: $reason{$node}' -S $start_time $duration`;
    # now output info to the user
    ret=''
    if 'reason' in conf:
      # list each node and the reason each is down
      for node in failed_nodes:
        ret = node+': '+reason[node]+'\n'
    else:
      # simply print the list of down node in range syntax
      ret = ','.join(failed_nodes)+'.'
    return ret
  # otherwise, don't print anything and exit with 0
  return 0
