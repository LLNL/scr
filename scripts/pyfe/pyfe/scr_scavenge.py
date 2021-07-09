#! /usr/bin/env python3

# scr_scavenge.py
# scavenge checkpoint files from cache to PFS

import os, sys

if 'pyfe' not in sys.path:
  sys.path.insert(0,'/'.join(os.path.dirname(__file__).split('/')[:-1]))
  print(sys.path)
  import pyfe

import argparse
from datetime import datetime
from time import time
from pyfe import scr_const, scr_common, scr_hostlist
from pyfe.scr_param import SCR_Param
from pyfe.scr_environment import SCR_Env
from pyfe.resmgr import AutoResourceManager
from pyfe.scr_common import tracefunction, runproc

# check for pdsh / (clustershell) errors in case any nodes should be retried

def scr_scavenge(nodeset_job=None, nodeset_up=None, nodeset_down=None, dataset_id=None, cntldir=None, prefixdir=None, verbose=False, scr_env=None):
  # check that we have a nodeset for the job and directories to read from / write to
  if nodeset_job is None or dataset_id is None or cntldir is None or prefixdir is None:
    return 1

  if verbose:
    sys.settrace(tracefunction)

  bindir = scr_const.X_BINDIR
  pdsh = scr_const.PDSH_EXE

  # TODO: need to be able to set these defaults via config settings somehow
  # for now just hardcode the values

  if scr_env is None:
    scr_env = SCR_Env()
  if scr_env.param is None:
    scr_env.param = SCR_Param()
  if scr_env.resmgr is None:
    scr_env.resmgr = AutoResourceManager()
  # lookup buffer size and crc flag via scr_param
  param = scr_env.param

  buf_size = os.environ.get('SCR_FILE_BUF_SIZE')
  if buf_size is None:
    buf_size = str(1024*1024)

  crc_flag = os.environ.get('SCR_CRC_ON_FLUSH')
  if crc_flag is None:
    crc_flag = '--crc'
  elif crc_flag == '0':
    crc_flag = ''

  start_time = int(time())

  # tag output files with jobid
  jobid = scr_env.resmgr.getjobid()
  if jobid is None:
    print('scr_scavenge: ERROR: Could not determine jobid.')
    return 1

  # read node set of job
  jobset = scr_env.conf['nodes']
  if jobset is None:
    jobset = scr_env.resmgr.conf['nodes']
    if jobset is None:
      print('scr_scavenge: ERROR: Could not determine nodeset.')
      return 1

  # get nodesets
  jobnodes  = scr_hostlist.expand(nodeset_job)
  upnodes = []
  downnodes = []
  if nodeset_down is not None:
    downnodes = scr_hostlist.expand(nodeset_down)
    upnodes   = scr_hostlist.diff(jobnodes, downnodes)
  elif nodeset_up is not None:
    upnodes   = scr_hostlist.expand(nodeset_up)
    downnodes = scr_hostlist.diff(jobnodes, upnodes)
  else:
    upnodes = jobnodes

  ##############################
  # format up and down node sets for pdsh command
  #################
  upnodes = scr_hostlist.compress(upnodes)
  downnodes_spaced = ' '.join(downnodes)

  # build the output filenames
  output = prefixdir+'/.scr/scr.dataset.'+dataset_id+'/scr_scavenge.pdsh.o'+jobid
  error  = prefixdir+'/.scr/scr.dataset.'+dataset_id+'/scr_scavenge.pdsh.e'+jobid

  # log the start of the scavenge operation
  scr_common.log(bindir=bindir, prefix=prefixdir, jobid=jobid, event_type='SCAVENGE_START', event_dset=dataset_id, event_start=str(start_time))

  # gather files via pdsh
  argv = scr_env.resmgr.get_scavenge_pdsh_cmd()
  print('scr_scavenge: '+str(int(time())))
  #argv = ['$pdsh','-Rexec','-f','256','-S','-w','$upnodes','srun','-n1','-N1','-w','%h','$bindir/scr_copy','--cntldir','$cntldir','--id','$dataset_id','--prefix','$prefixdir','--buf','$buf_size','$crc_flag','$downnodes_spaced']
  delargs = []
  for i,arg in enumerate(argv):
    if arg[0]!='$':
      continue
    if arg=='$pdsh':
      argv[i] = pdsh
    elif arg=='$upnodes':
      argv[i] = upnodes
    elif arg=='$cntldir':
      argv[i] = cntldir
    elif arg=='$dataset_id':
      argv[i] = dataset_id
    elif arg=='$prefixdir':
      argv[i] = prefixdir
    elif arg=='$buf_size':
      argv[i] = buf_size
    elif arg=='$crc_flag':
      if crc_flag!='':
        argv[i]=crc_flag
      else:
        delargs.append(i)
    elif arg=='$downnodes_spaced':
      if downnodes_spaced!='':
        argv[i]=downnodes_spaced
      else:
        delargs.append(i)
    elif '$bindir' in arg:
      pos = arg.find('$bindir')
      argv[i] = arg[:pos]+bindir+arg[pos+7:]
  # delete unused arguments from the back to avoid index issues
  for i in range(len(delargs)-1,-1,-1):
    del argv[delargs[i]]
  print('scr_scavenge: '+' '.join(argv))
  #`$pdsh -Rexec -f 256 -S -w '$upnodes' srun -n1 -N1 -w %h $bindir/scr_copy --cntldir $cntldir --id $dset --prefix $prefixdir --buf $buf_size $crc_flag $downnodes_spaced`;
  consoleout = runproc(argv=argv,getstdout=True,getstderr=True)[0]

  # print pdsh output to screen
  try:
    with open(output,'w') as outfile:
      outfile.write(consoleout[0])
    if verbose:
      print('scr_scavenge: stdout: cat '+output)
      print(consoleout[0])
  except Exception as e:
    print(str(e))
    print('scr_scavenge: ERROR: Unable to write stdout to \"'+output+'\"')
  try:
    with open(error,'w') as outfile:
      outfile.write(consoleout[1])
    if verbose:
      print('scr_scavenge: stderr: cat '+error)
      print(consoleout[1])
  except Exception as e:
    print(str(e))
    print('scr_scavenge: ERROR: Unable to write stderr to \"'+error+'\"')

  # TODO: if we knew the total bytes, we could register a transfer here in addition to an event
  # get a timestamp for logging timing values
  end_time = int(time())
  diff_time = end_time - start_time
  scr_common.log(bindir=bindir, prefix=prefixdir, jobid=jobid, event_type='SCAVENGE_END', event_dset=dataset_id, event_start=str(start_time), event_secs=str(diff_time))
  return 0

if __name__=='__main__':
  parser = argparse.ArgumentParser(add_help=False, argument_default=argparse.SUPPRESS, prog='scr_scavenge')
  parser.add_argument('-h','--help', action='store_true', help='Show this help message and exit.')
  parser.add_argument('-v','--verbose', action='store_true', default=False, help='Verbose output.')
  parser.add_argument('-j','--jobset', metavar='<nodeset>', type=str, default=None, help='Specify the nodeset.')
  parser.add_argument('-u','--up', metavar='<nodeset>', type=str, default=None, help='Specify up nodes.')
  parser.add_argument('-d','--down', metavar='<nodeset>', type=str, default=None, help='Specify down nodes.')
  parser.add_argument('-i','--id', metavar='<id>', type=str, default=None, help='Specify the dataset id.')
  parser.add_argument('-f','--from', metavar='<dir>', type=str, default=None, help='The control directory.')
  parser.add_argument('-t','--to', metavar='<dir>', type=str, default=None, help='The prefix directory.')
  args = vars(parser.parse_args())
  if 'help' in args:
    parser.print_help()
  elif args['jobset'] is None or args['id'] is None or args['from'] is None or args['to'] is None:
    parser.print_help()
    print('Required arguments: --jobset --id --from --to')
  else:
    ret = scr_scavenge(nodeset_job=args['jobset'], nodeset_up=args['up'], nodeset_down=args['down'], dataset_id=args['id'], cntldir=args['from'], prefixdir=args['to'], verbose=args['verbose'], scr_env=None)
    print('scr_scavenge returned '+str(ret))
