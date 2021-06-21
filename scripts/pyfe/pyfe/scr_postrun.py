#! /usr/bin/env python

# scr_postrun.py

# Run this script after the final run in a job allocation
# to scavenge files from cache to parallel file system.

import argparse, os, scr_const, sys
from datetime import datetime
from time import time
from scr_common import tracefunction, scr_prefix, runproc
from scr_scavenge import scr_scavenge
from scr_list_down_nodes import scr_list_down_nodes
from scr_glob_hosts import scr_glob_hosts
from scr_list_dir import scr_list_dir
from scr_env import SCR_Env

def scr_postrun(prefix_dir=None,scr_env=None):
  # if SCR is disabled, immediately exit
  val = os.environ.get('SCR_ENABLE')
  if val is not None and val=='0':
    return 0

  # if SCR_DEBUG is set > 0, turn on verbosity
  verbose=False
  val = os.environ.get('SCR_DEBUG')
  if val is not None and int(val)>0:
    sys.settrace(tracefunction)
    verbose=True

  # record the start time for timing purposes
  start_time=datetime.now()
  start_secs=int(time())

  bindir=scr_const.X_BINDIR

  pardir = ''
  # pass prefix via command line
  if prefix_dir is not None:
    pardir=prefix_dir
  else:
    pardir=scr_prefix()

  # check that we have the parallel file system prefix
  if pardir=='':
    return 1

  # all parameters checked out, start normal output
  print('scr_postrun: Started: '+str(datetime.now()))

  if scr_env is None:
    scr_env = SCR_Env()
  # get our nodeset for this job
  nodelist_env = os.environ.get('SCR_NODELIST')
  if nodelist_env is None:
    nodelist_env = scr_env.get_job_nodes()
    if nodelist_env is None:
      print('scr_postrun: ERROR: Could not identify nodeset')
      return 1
    os.environ['SCR_NODELIST'] = nodelist_env
  scr_nodelist = os.environ.get('SCR_NODELIST')
  # identify what nodes are still up
  upnodes=scr_nodelist
  downnodes = scr_list_down_nodes(nodeset=upnodes,scr_env=scr_env)
  if type(downnodes) is int:
    downnodes = ''
    #if downnodes==1: # returned error
    #  return 1 # probably should return error (?)
    #else: #returned 0, no error and no down nodes
    #  downnodes = ''
  else: # returned a list of down nodes
    upnodes = scr_glob_hosts(minus=upnodes+':'+downnodes)
  print('scr_postrun: UPNODES:   '+upnodes)

  # if there is at least one remaining up node, attempt to scavenge
  ret=1
  if upnodes!='':
    cntldir=scr_list_dir(runcmd='control',scr_env=scr_env)
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
    print('scr_postrun: Looking for output sets')
    failed_dataset=0
    argv = [bindir+'/scr_flush_file','--dir',pardir,'--list-output']
    output_list, returncode = runproc(argv=argv,getstdout=True)
    if returncode!=0:
      print('scr_postrun: Found no output set to scavenge')
    else:
      argv.append('') # make len(argv) == 5
      #### Need the format of the scr_flush_file output ####
      #### This is just looping over characters ####
      if '\n' in outputlist:
        outputlist = outputlist.split('\n')
      elif ',' in outputlist:
        outputlist = outputlist.split(',')
      for d in output_list:
        # determine whether this dataset needs to be flushed
        argv[3]='--need-flush'
        argv[4]=d
        returncode = runproc(argv=argv)[1]
        if returncode!=0:
          # dataset has already been flushed, go to the next one
          print('scr_postrun: Dataset '+d+' has already been flushed')
          continue
        print('scr_postrun: Attempting to scavenge dataset '+d)

        # add $d to ATTEMPTED list
        attempted.append(d)

        # get dataset name
        argv[3]='--name'
        dsetname, returncode = runproc(argv=argv,getstdout=True)
        if returncode!=0:
          # got a dataset to flush, but failed to get name
          print('scr_postrun: Failed to read name of dataset '+d)
          failed_dataset=d
          break
        # build full path to dataset directory
        datadir=pardir+'/.scr/scr.dataset.'+d
        os.makedirs(datadir,exist_ok=True)

        # Gather files from cache to parallel file system
        print('scr_postrun: Scavenging files from cache for '+dsetname+' to '+datadir)
        print('scr_postrun: '+bindir+'/scr_scavenge '+('--verbose ' if verbose else '')+'--id '+d+' --from '+cntldir+' --to '+pardir+' --jobset '+scr_nodelist+' --up '+upnodes)
        if scr_scavenge(nodeset_job=scr_nodelist, nodeset_up=upnodes, dataset_id=d, cntldir=cntldir, prefixdir=pardir, verbose=verbose, scr_env=scr_env)!=1:
          print('scr_postrun: Done scavenging files from cache for '+dsetname+' to '+datadir)
        else:
          print('scr_postrun: ERROR: Scavenge files from cache for '+dsetname+' to '+datadir)

        # check that gathered set is complete,
        # if not, don't update current marker
        #update_current=1
        print('scr_postrun: Checking that dataset is complete')
        print(bindir+'/scr_index --prefix '+pardir+' --build '+d)
        index_argv = [bindir+'/scr_index','--prefix',pardir,'--build',d]
        returncode = runproc(argv=argv)[1]
        if returncode!=0:
          # failed to get dataset, stop trying for later sets
          failed_dataset=d
          break
        # remember that we scavenged this dataset in case we try again below
        succeeded.append(d)
        print('scr_postrun: Scavenged dataset '+dsetname+' successfully')

    # check whether we have a dataset set to flush
    print('scr_postrun: Looking for most recent checkpoint')
    argv = [bindir+'/scr_flush_file','--dir',pardir,'--list-ckpt','--before',failed_dataset]
    ckpt_list, returncode = runproc(argv=argv,getstdout=True)
    if returncode!=0:
      print('scr_postrun: Found no checkpoint to scavenge')
    else:
      argv = [bindir+'/scr_flush_file','--dir',pardir,'--name','']
      for d in ckpt_list:
        if d in attempted:
          if d in succeeded:
            # already got this one above, update current, and finish
            argv[4] = d
            dsetname, returncode = runproc(argv=argv,getstdout=True)
            if returncode==0:
              print('scr_postrun: Already scavenged checkpoint dataset '+d)
              print('scr_postrun: Updating current marker in index to '+dsetname)
              index_argv = [bindir+'/scr_index','--prefix',pardir,'--current',dsetname]
              runproc(argv=index_argv)
              ret=0
              break
          else:
            # already tried and failed, skip this dataset
            print('scr_postrun: Skipping checkpoint dataset '+d+', since already failed to scavenge')
            continue

        # we have a dataset, check whether it still needs to be flushed

        argv[3]='--need-flush'
        argv[4]=d
        returncode = runproc(argv=argv)[1]
        if returncode!=0:
          # found a dataset that has already been flushed, we can quit
          print('scr_postrun: Checkpoint dataset '+d+' has already been flushed')
          ret=0
          break
        print('scr_postrun: Attempting to scavenge checkpoint dataset '+d)

        # get dataset name
        argv[3]='--name'
        dsetname, returncode = runproc(argv=argv,getstdout=True)
        if returncode!=0:
          # got a dataset to flush, but failed to get name
          print('scr_postrun: Failed to read name of checkpoint dataset '+d)
          continue
        # build full path to dataset directory
        datadir=pardir+'/.scr/scr.dataset.'+d
        os.makedirs(datadir,exist_ok=True)

        # Gather files from cache to parallel file system
        print('scr_postrun: Scavenging files from cache for checkpoint '+dsetname+' to '+datadir)
        print('scr_postrun: '+bindir+'/scr_scavenge '+('--verbose ' if verbose else '')+'--id '+d+' --from '+cntldir+' --to '+pardir+' --jobset '+scr_nodelist+' --up '+upnodes)
        if scr_scavenge(nodeset_job=scr_nodelist, nodeset_up=upnodes, dataset_id=d, cntldir=cntldir, prefixdir=pardir, verbose=verbose, scr_env=scr_env) != 1:
          print('scr_postrun: Done scavenging files from cache for '+dsetname+' to '+datadir)
        else:
          print('scr_postrun: ERROR: Scavenge files from cache for '+dsetname+' to '+datadir)

        # check that gathered set is complete,
        # if not, don't update current marker
        print('scr_postrun: Checking that dataset is complete')
        print(bindir+'/scr_index --prefix '+pardir+' --build '+d)
        argv = [bindir+'/scr_index','--prefix',pardir,'--build',d]
        returncode = runproc(argv=argv)[1]
        if returncode!=0:
          # incomplete dataset, don't update current marker
          #update_current=0
          pass

        # if the set is complete, update the current marker
        else:
          # make the new current
          print('scr_postrun: Updating current marker in index to '+dsetname)
          argv[3]='--current'
          argv[4]=dsetname
          runproc(argv=argv)

          # just completed scavenging this dataset, so quit
          ret=0
          break

  # print the timing info
  end_time=datetime.now()
  end_secs=int(time())
  run_secs=end_secs - start_secs
  print('scr_postrun: Ended: '+str(end_time))
  print('scr_postrun: secs: '+str(run_secs))

  # print the exit code and exit
  print('scr_postrun: exit code: '+str(ret))
  return ret

if __name__=='__main__':
  parser = argparse.ArgumentParser(add_help=False, argument_default=argparse.SUPPRESS, prog='scr_postrun')
  parser.add_argument('-h','--help', action='store_true', help='Show this help message and exit.')
  parser.add_argument('-p','--prefix', metavar='<dir>', type=str, default=None, help='Specify the prefix directory.')
  args = vars(parser.parse_args())
  if 'help' in args:
    parser.print_help()
  else:
    scr_postrun(prefix_dir=args['prefix'])

