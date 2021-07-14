#! /usr/bin/env python3

# scr_scavenge.py
# scavenge checkpoint files from cache to PFS

import os, sys

if 'pyfe' not in sys.path:
  sys.path.insert(0,'/'.join(os.path.realpath(__file__).split('/')[:-2]))
  import pyfe

import argparse
from datetime import datetime
from time import time
from pyfe import scr_const, scr_common
from pyfe.scr_param import SCR_Param
from pyfe.scr_environment import SCR_Env
from pyfe.resmgr import AutoResourceManager
from pyfe.scr_common import tracefunction, runproc

# check for pdsh / (clustershell) errors in case any nodes should be retried

def scr_scavenge(nodeset_job=None, nodeset_up='', nodeset_down='', dataset_id=None, cntldir=None, prefixdir=None, verbose=False, scr_env=None):
  # check that we have a nodeset for the job and directories to read from / write to
  if nodeset_job is None or dataset_id is None or cntldir is None or prefixdir is None:
    return 1

  if verbose:
    sys.settrace(tracefunction)

  bindir = scr_const.X_BINDIR

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

  # build the output filenames
  output = prefixdir+'/.scr/scr.dataset.'+dataset_id+'/scr_scavenge.pdsh.o'+jobid
  error  = prefixdir+'/.scr/scr.dataset.'+dataset_id+'/scr_scavenge.pdsh.e'+jobid

  # log the start of the scavenge operation
  scr_common.log(bindir=bindir, prefix=prefixdir, jobid=jobid, event_type='SCAVENGE_START', event_dset=dataset_id, event_start=str(start_time))

  print('scr_scavenge: '+str(int(time())))
  # have the resmgr class gather files via pdsh or clustershell
  consoleout = resmgr.scavenge_files(prog=bindir+'/scr_copy', upnodes=nodeset_up, downnodes=nodeset_down, cntldir=cntldir, dataset_id=dataset_id, prefixdir=prefixdir, buf_size=buf_size, crc_flag=crc_flag)

  # print outputs to screen
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
