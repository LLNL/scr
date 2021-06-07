#! /usr/bin/env python

import os, sys
import numpy as np
from datetime import datetime
from scr_param import SCR_Param
from scr_env import SCR_Env
import scr_const
from scr_common import tracefunction

# scavenge checkpoint files from cache to PFS
# check for pdsh / (clustershell) errors in case any nodes should be retried

# Usage: $prog [--jobset <nodeset>] [--up <nodeset> | --down <nodeset>] --id <id> --from <dir> --to <dir>

def getoptkey(opt):
  if opt=='-j' or opt=='jobset':
    return 'nodeset_job'
  if opt=='-u' or opt=='--up':
    return 'nodeset_up'
  if opt=='-d' or opt=='--down':
    return 'nodeset_down'
  if opt=='-i' or opt=='--id':
    return 'dataset_id'
  if opt=='-f' or opt=='--from':
    return 'dir_from'
  if opt=='-t' or opt=='--to':
    return 'dir_to'
  if opt=='-v' or opt=='--verbose':
    return 'verbose'
  return None

def print_usage(prog):
  print('')
  print('  Usage:  '+prog+' [--jobset <nodeset>] [--up <nodeset> | --down <nodeset>] --id <id> --from <dir> --to <dir>\n')

def scr_scavenge(argv,scr_env=None):
  # the verbose flag is used at the bottom of this method
  verbose = False

  bindir = scr_const.X_BINDIR
  prog = 'scr_scavenge'
  pdsh = scr_const.PDSH_EXE

  # TODO: need to be able to set these defaults via config settings somehow
  # for now just hardcode the values

  # lookup buffer size and crc flag via scr_param
  param = SCR_Param();

  buf_size = os.environ.get('SCR_FILE_BUF_SIZE')
  if buf_size is None:
    buf_size = str(1024*1024)

  crc_flag = os.environ.get('SCR_CRC_ON_FLUSH')
  if crc_flag is None:
    crc_flag = '--crc'
  elif crc_flag == '0':
    crc_flag = ''

  start_time = datetime.now()

  # tag output files with jobid
  if scr_env is None:
    scr_env = SCR_Env('SLURM') ### set environment
  jobid = scr_env.getjobid()
  if jobid is None:
    print(prog'+: ERROR: Could not determine jobid.')
    return 1

  # read node set of job
  jobset = scr_env.conf['nodes']
  if jobset is None:
    print(prog+': ERROR: Could not determine nodeset.')
    return 1

  # read in command line arguments
  conf = {}
  #conf[nodeset_job] = $jobset
  #conf{nodeset_up}   = undef;
  #conf{nodeset_down} = undef;
  #conf{dataset_id}   = undef;
  #conf{dir_from}     = undef;
  #conf{dir_to}       = undef;
  #conf{verbose}      = 0;

  skip=False
  for i in range(1,len(argv)):
    if skip==True:
      skip=False
    elif '=' in argv[i]:
        vals = argv[i].split('=')
        vals[0] = getoptkey(vals[0])
        if vals[0] is None:
          print_usage(prog)
          return 1
        if vals[0]=='verbose':
          sys.settrace(tracefunction)
          verbose=True
        else:
          conf[vals[0]]=vals[1]
      else:
        opt=getoptkey(argv[i])
        if opt is None or i+1==len(agv):
          print_usage()
          return 1
        if opt=='verbose':
          sys.settrace(tracefunction)
          verbose=True
          opt=''
        else:
          conf[opt]=argv[i+1]
          skip=True
    else: #if opt != '':
      conf[opt]=sys.argv[i]
      opt=''

  # check that we have a nodeset for the job and directories to read from / write to
  if 'nodeset_job' not in conf or 'dataset_id' not in conf or 'dir_from' not in conf or 'dir_to' not in conf:
    print_usage()
    return 1

  # get directories
  cntldir   = conf['dir_from']
  prefixdir = conf['dir_to']

  # get nodesets
  #jobnodes  = scr_hostlist::expand($conf{nodeset_job});
  jobnodes = ['rhea2','rhea3','rhea4']
  upnodes = []
  downnodes = []
  if 'nodeset_down' in conf:
    downnodes.append('rhea2')
    #@downnodes = scr_hostlist::expand($conf{nodeset_down});
    upnodes = np.setdiff1d(jobnodes,downnodes)
    #@upnodes   = scr_hostlist::diff(\@jobnodes, \@downnodes);
  elif 'nodeset_up' in conf:
    upnodes.append('rhea2')
    #@upnodes   = scr_hostlist::expand($conf{nodeset_up});
    downnodes = np.setdiff1d(jobnodes,upnodes)
    #@downnodes = scr_hostlist::diff(\@jobnodes, \@upnodes);
  else:
    upnodes = jobnodes

  ##############################
  # format up and down node sets for pdsh command
  #################
  #my $upnodes = scr_hostlist::compress(@upnodes);
  downnodes_spaced = ' '.join(downnodes)

  # add dataset id option if one was specified (this is required from above)
  # set the dataset flag
  dset = conf['dataset_id']

  # build the output filenames
  output = prefixdir+'/.scr/scr.dataset.$dset/$prog.pdsh.o.'+jobid #jobid needs to be set above #########
  error  = prefixdir+'/.scr/scr.dataset.$dset/$prog.pdsh.e.'+jobid

  cmd = ''

  # log the start of the scavenge operation
  argv = [bindir+'/scr_log_event','-i',jobid,'-p',prefixdir,'-T','SCAVENGE_START','-D',dset,'-S',str(start_time)] # start time string val... ############
  #'$bindir/scr_log_event -i $jobid -p $prefixdir -T 'SCAVENGE_START' -D $dset -S $start_time';
  runproc = subprocess.Popen(args=argv,bufsize=1,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True,universal_newlines=True)
  out,err = runproc.communicate()
  if runproc.returncode!=0:
    print(err)
    print('scr_log_event returned '+str(runproc.returncode))
    return 1

  # gather files via pdsh
  #### need to fix %h #########
  argv = ['srun','-n','1','-N','1','-w','%h',bindir+'/scr_copy','--cntldir',cntldir,'--id',dset,'--prefix',prefixdir,'--buf',buf_size,crc_flag,downnodes_spaced]
  #$cmd = "srun -n 1 -N 1 -w %h $bindir/scr_copy --cntldir $cntldir --id $dset --prefix $prefixdir --buf $buf_size $crc_flag $downnodes_spaced";
  runproc = subprocess.Popen(args=argv,bufsize=1,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True,universal_newlines=True)
  out,err = runproc.communicate()
  if runproc.returncode!=0:
    print(err)
    print('scr_copy returned '+str(runproc.returncode))
    return 1

  print(prog+': '+str(datetime.now()))

  ##### this top comand (where output redirected to file) was commented out ?
  # Does not work with "$cmd" for some reason using -Rexec
  #print "$prog: $pdsh -Rexec -f 256 -S -w '$upnodes' \"$cmd\" >$output 2>$error\n";
  #             '$pdsh -Rexec-f 256 -S -w '$upnodes'  "$cmd"  >$output 2>$error';
  #### need to expand %h ######
  argv = [pdsh,'-Rexec','-f','256','-S','-w',upnodes,'srun','-n1','-N1','-w','%h',bindir+'/scr_copy','--cntldir',cntldir,'--id',dset,'--prefix',prefixdir,'--buf',buf_size,crc_flag,downnodes_spaced]
  #print "$prog: $pdsh -Rexec -f 256 -S -w '$upnodes' srun -n1 -N1 -w %h 
  #$bindir/scr_copy --cntldir $cntldir --id $dset --prefix $prefixdir --buf $buf_size $crc_flag $downnodes_spaced";
  #             '$pdsh -Rexec -f 256 -S -w '$upnodes' srun -n1 -N1 -w %h 
  #$bindir/scr_copy --cntldir $cntldir --id $dset --prefix $prefixdir --buf $buf_size $crc_flag $downnodes_spaced';
  runproc = subprocess.Popen(args=argv,bufsize=1,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True,universal_newlines=True)
  out,err = runproc.communicate()
  if runproc.returncode!=0:
    print(err)
    print('pdsh returned '+str(runproc.returncode))
    #sys.exit(1)
  # print pdsh output to screen
  if verbose==True:
    if len(out)>0:
      print('pdsh: stdout: cat '+output)
      print(out)
    if len(err)>0:
      print('pdsh: stderr: cat '+error)
      print(err)

  # TODO: if we knew the total bytes, we could register a transfer here in addition to an event
  # get a timestamp for logging timing values
  end_time = datetime.now()
  diff_time = end_time - start_time
  argv = [bindir+'/scr_log_event','-i',jobid,'-p',prefixdir,'-T','SCAVENGE_END','-D',dset,'-S',str(start_time),'-L',str(diff_time)] ##### need string ( time )
  #'$bindir/scr_log_event -i $jobid -p $prefixdir -T 'SCAVENGE_END' -D $dset -S $start_time -L $diff_time';
  runproc = subprocess.Popen(args=argv,bufsize=1,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True,universal_newlines=True)
  runproc.communicate()
  return 0
