#! /usr/bin/env python

# scr_postrun.py

# Run this script after the final run in a job allocation
# to scavenge files from cache to parallel file system.

import os, scr_const, sys
from datetime import datetime
from scr_common import tracefunction, getconf, scr_prefix
from scr_scavenge import scr_scavenge
from scr_list_down_nodes import scr_list_down_nodes
from scr_glob_hosts import scr_glob_hosts
from scr_list_dir import scr_list_dir
from scr_env import SCR_Env

def print_usage(prog):
  print('Usage: '+prog+' [-p prefix_dir]')

def scr_postrun(argv,scr_env=None):
  # if SCR is disabled, immediately exit
  val = os.environ.get('SCR_ENABLE')
  if val is not None and val=='0':
    return 0

  # if SCR_DEBUG is set > 0, turn on verbosity
  verbose=''
  val = os.environ.get('SCR_DEBUG')
  if val is not None and int(val)>0:
    sys.settrace(tracefunction)
    verbose='--verbose'

  # record the start time for timing purposes
  start_time=datetime.now()
  start_secs=start_time.time().second

  bindir=scr_const.X_BINDIR

  prog='scr_postrun'

  # pass prefix via command line
  pardir=scr_prefix()
  conf = getconf(argv,keyvals={'-p':'prefix'})
  if conf is None:
    print_usage(prog)
    return 1
  if 'prefix' in conf:
    pardir=conf['prefix']

  # check that we have the parallel file system prefix
  if pardir=='':
    print_usage(prog)
    return 1

  # all parameters checked out, start normal output
  print(prog+': Started: '+str(start_time))

  # ensure scr_env is set
  if scr_env is None:
    scr_env = SCR_Env()
  # get our nodeset for this job
  nodelist_env = os.environ.get('SCR_NODELIST')
  if nodelist_env is None:
    nodelist_env = scr_env.getnodelist()
    if nodelist_env is None:
      print(prog+': ERROR: Could not identify nodeset')
      return 1
    os.environ['SCR_NODELIST'] = nodelist_env
  SCR_NODELIST = os.environ.get('SCR_NODELIST')
  # identify what nodes are still up
  upnodes=nodelist_env
  downnodes = scr_list_down_nodes(upnodes,scr_env)
  if type(downnodes) is int:
    if downnodes==1: # returned error
      return 1 # probably should return error
    # else: returned 0, no error and no down nodes
  else: # returned a list of down nodes
    upnodes = scr_glob_hosts(['--minus',upnodes+':'+downnodes])
  print(prog+': UPNODES:   '+upnodes)

  # if there is at least one remaining up node, attempt to scavenge
  ret=1
  if upnodes!='':
    cntldir=scr_list_dir(['control'],scr_env)
    # TODO: check that we have a control directory

    # TODODSET: avoid scavenging things unless it's in this list
    # get list of possible datasets
    #  dataset_list=`$bindir/scr_inspect --up $UPNODES --from $cntldir`
    #  if [ $? -eq 0 ] ; then
    #  else
    #    echo "$prog: Failed to inspect cache or cannot scavenge any datasets"
    #  fi

    # array to track which datasets we tried to get
    attempted = []

    # array to track datasets we got
    succeeded = []

    # scavenge all output sets in ascending order,
    # track the id of the first one we fail to get
    print(prog+': Looking for output sets')
    failed_dataset=0
    argv = [bindir+'/scr_flush_file','--dir',pardir,'--list-output']
    runproc = subprocess.Popen(args=argv, bufsize=1, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
    output_list = runproc.communicate()[0]
    if runproc.returncode!=0:
      print(prog+': Found no output set to scavenge')
    else:
      argv.append('') # make len(argv) == 5
      #### Need the format of the scr_flush_file output ####
      #### This is just looping over characters ####
      for d in output_list:
        # determine whether this dataset needs to be flushed
        argv[3]='--need-flush'
        argv[4]=d
        runproc = subprocess.Popen(args=argv, bufsize=1, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
        runproc.communicate()
        if runproc.returncode!=0:
          # dataset has already been flushed, go to the next one
          print(prog+': Dataset '+d+' has already been flushed')
          continue
        print(prog+': Attempting to scavenge dataset '+d)

        # add $d to ATTEMPTED list
        attempted.append(d)

        # get dataset name
        argv[3]='--name'
        runproc = subprocess.Popen(args=argv, bufsize=1, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
        dsetname = runproc.communicate()[0]
        if runproc.returncode!=0:
          # got a dataset to flush, but failed to get name
          print(prog+': Failed to read name of dataset '+d)
          failed_dataset=d
          break
        # build full path to dataset directory
        datadir=pardir+'/.scr/scr.dataset.'+d
        os.makedirs(datadir,exist_ok=True)

        # Gather files from cache to parallel file system
        print(prog+': Scavenging files from cache for '+dsetname+' to '+datadir)
        print(prog+': '+bindir+'/scr_scavenge '+verbose+' --id '+d+' --from '+cntldir+' --to '+pardir+' --jobset '+SCR_NODELIST+' --up '+upnodes)
        scavenge_argv = ['--id',d,'--from',cntldir,'--to',pardir,'--jobset',SCR_NODELIST,'--up',upnodes]
        if verbose!='':
          scavenge_argv.append(verbose)
        if scr_scavenge(scavenge_argv,scr_env)!=1:
          print(prog+': Done scavenging files from cache for '+dsetname+' to '+datadir)
        else:
          print(prog+': ERROR: Scavenge files from cache for '+dsetname+' to '+datadir)

        # check that gathered set is complete,
        # if not, don't update current marker
        update_current=1
        print(prog+': Checking that dataset is complete')
        print(bindir+'/scr_index --prefix '+pardir+' --build '+d)
        index_argv = [bindir+'/scr_index','--prefix',pardir,'--build',d]
        runproc = subprocess.Popen(args=argv, bufsize=1, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
        runproc.communicate()
        if runproc.returncode!=0:
          # failed to get dataset, stop trying for later sets
          failed_dataset=d
          break
        # remember that we scavenged this dataset in case we try again below
        succeeded.append(d)
        print(prog+': Scavenged dataset '+dsetname+' successfully')

    # check whether we have a dataset set to flush
    print(prog+': Looking for most recent checkpoint')
    argv = [bindir+'/scr_flush_file','--dir',pardir,'--list-ckpt','--before',failed_dataset]
    runproc = subprocess.Popen(args=argv, bufsize=1, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
    ckpt_list = runproc.communicate()[0]
    if runproc.returncode!=0:
      print(prog+': Found no checkpoint to scavenge')
    else:
      argv = [bindir+'/scr_flush_file','--dir',pardir,'--name','']
      for d in ckpt_list:
        if d in attempted:
          if d in succeeded:
            # already got this one above, update current, and finish
            argv[4] = d
            runproc = subprocess.Popen(args=argv, bufsize=1, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
            dsetname = runproc.communicate()[0]
            if runproc.returncode==0:
              print(prog+': Already scavenged checkpoint dataset '+d)
              print(prog+': Updating current marker in index to '+dsetname)
              index_argv = [bindir+'/scr_index','--prefix',pardir,'--current',dsetname]
              runproc = subprocess.Popen(args=index_argv, bufsize=1, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
              runproc.communicate()
              ret=0
              break
          else:
            # already tried and failed, skip this dataset
            print(prog+': Skipping checkpoint dataset '+d+', since already failed to scavenge')
            continue

        # we have a dataset, check whether it still needs to be flushed

        argv[3]='--need-flush'
        argv[4]=d
        runproc = subprocess.Popen(args=argv, bufsize=1, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
        runproc.communicate()
        if runproc.returncode!=0:
          # found a dataset that has already been flushed, we can quit
          print(prog+': Checkpoint dataset '+d+' has already been flushed')
          ret=0
          break
        print(prog+': Attempting to scavenge checkpoint dataset '+d)

        # get dataset name
        argv[3]='--name'
        runproc = subprocess.Popen(args=argv, bufsize=1, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
        dsetname = runproc.communicate()[0]
        if runproc.returncode!=0:
          # got a dataset to flush, but failed to get name
          print(prog+': Failed to read name of checkpoint dataset '+d)
          continue
        # build full path to dataset directory
        datadir=pardir+'/.scr/scr.dataset.'+d
        os.makedirs(datadir,exist_ok=True)

        # Gather files from cache to parallel file system
        print(prog+': Scavenging files from cache for checkpoint $dsetname to '+datadir)
        print(prog+': '+bindir+'/scr_scavenge '+verbose+' --id '+d+' --from '+cntldir+' --to '+pardir+' --jobset '+SCR_NODELIST+' --up '+upnodes)
        scavenge_argv = ['--id',d,'--from',cntldir,'--to',pardir,'--jobset',SCR_NODELIST,'--up',upnodes]
        if verbose!='':
          scavenge_argv.append(verbose)
        if scr_scavenge(scavenge_argv,scr_env)!=1:
          print(prog+': Done scavenging files from cache for '+dsetname+' to '+datadir)
        else:
          print(prog+': ERROR: Scavenge files from cache for '+dsetname+' to '+datadir)

        # check that gathered set is complete,
        # if not, don't update current marker
        update_current=1
        print(prog+': Checking that dataset is complete')
        print(bindir+'/scr_index --prefix '+pardir+' --build '+d)
        argv = [bindir+'/scr_index','--prefix',pardir,'--build',d]
        runproc = subprocess.Popen(args=argv, bufsize=1, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
        runproc.communicate()
        if runproc.returncode!=0:
          # incomplete dataset, don't update current marker
          update_current=0

        # if the set is complete, update the current marker
        if update_current == 1:
          # make the new current
          print(prog+': Updating current marker in index to '+dsetname)
          argv[3]='--current'
          argv[4]=dsetname
          runproc = subprocess.Popen(args=argv, bufsize=1, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
          runproc.communicate()

          # just completed scavenging this dataset, so quit
          ret=0
          break

  # print the timing info
  end_time=datetime.now()
  end_secs=end_time.time().second
  run_secs=end_secs - start_secs
  print(prog+': Ended: '+str(end_time))
  print(prog+': secs: '+str(run_secs))

  # print the exit code and exit
  print(prog+': exit code: '+str(ret))
  return ret

if __name__=='__main__':
  ret = scr_postrun(sys.argv[1:])
  print('scr_postrun returned '+str(ret))

