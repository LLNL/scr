#! /usr/bin/env python

import argparse, os, sys
from datetime import datetime
from time import time
from pyfe import scr_const, scr_common, scr_hostlist
from pyfe.scr_param import SCR_Param
from pyfe.env.scr_env import SCR_Env
from pyfe.scr_common import tracefunction, runproc

# scavenge checkpoint files from cache to PFS
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

  # lookup buffer size and crc flag via scr_param
  param = SCR_Param()

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
  if scr_env is None:
    scr_env = SCR_Env()
  jobid = scr_env.getjobid()
  if jobid is None:
    print('scr_scavenge: ERROR: Could not determine jobid.')
    return 1

  # read node set of job
  jobset = scr_env.conf['nodes']
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
  returncode = scr_common.log(bindir=bindir, prefix=prefixdir, jobid=jobid, event_type='SCAVENGE_START', event_dset=dataset_id, event_start=str(start_time))

  # gather files via pdsh
  print('scr_scavenge: '+str(int(time())))
  argv = [pdsh,'-Rexec','-f','256','-S','-w',upnodes,'srun','-n1','-N1','-w','%h',bindir+'/scr_copy','--cntldir',cntldir,'--id',dataset_id,'--prefix',prefixdir,'--buf',buf_size]
  if crc_flag!='':
    argv.extend([crc_flag])
  argv.extend([downnodes_spaced])
  print('scr_scavenge: '+' '.join(argv))
  #`$pdsh -Rexec -f 256 -S -w '$upnodes' srun -n1 -N1 -w %h $bindir/scr_copy --cntldir $cntldir --id $dset --prefix $prefixdir --buf $buf_size $crc_flag $downnodes_spaced`;
  runproc(argv=argv,getstdout=True,getstderr=True)

  # print pdsh output to screen
  if verbose:
    try:
      print('scr_scavenge: stdout: cat '+output)
      with open(output,'r') as infile:
        for line in infile.readlines():
          print(line.rstrip())
    except:
      pass
    try:
      print('scr_scavenge: stderr: cat '+error)
      with open(error,'r') as infile:
        for line in infile.readlines():
          print(line.rstrip())
    except:
      pass

  # TODO: if we knew the total bytes, we could register a transfer here in addition to an event
  # get a timestamp for logging timing values
  end_time = int(time())
  diff_time = end_time - start_time
  scr_common.log(bindir=bindir, prefix=prefixdir, jobid=jobid, event_type='SCAVENGE_END', event_dset=dset, event_start=str(start_time), event_secs=str(diff_time))
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
