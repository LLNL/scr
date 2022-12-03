#! @Python_EXECUTABLE@

# postrun.py

# This script can run after the final run in a job allocation
# to scavenge files from cache to parallel file system.

import os, sys
from datetime import datetime
from time import time
from scrjob import scr_const
from scrjob.list_dir import list_dir
from scrjob.scr_scavenge import scr_scavenge
from scrjob.list_down_nodes import list_down_nodes
from scrjob.scr_glob_hosts import scr_glob_hosts
from scrjob.cli import SCRIndex, SCRFlushFile


def postrun(prefix_dir=None, scr_env=None, verbose=False, log=None):
  """this method is called after all runs has completed, before scr_run.py exits

  Determine whether there are datasets to scavenge, and perform scavenge operations

  Returns
  -------
  int     0 on success
          1 on error
  """
  if scr_env is None or scr_env.resmgr is None:
    return 1

  # if SCR is disabled, immediately exit
  val = os.environ.get('SCR_ENABLE')
  if val is not None and val == '0':
    return 0

  # record the start time for timing purposes
  start_time = datetime.now()
  start_secs = int(time())

  bindir = scr_const.X_BINDIR

  pardir = ''
  # pass prefix via command line
  if prefix_dir is not None:
    pardir = prefix_dir
  if not pardir:
    pardir = scr_env.get_prefix()

  # check that we have the parallel file system prefix
  if pardir == '':
    return 1

  scr_index = SCRIndex(pardir)
  scr_flush_file = SCRFlushFile(pardir)

  # all parameters checked out, start normal output
  print('scr_postrun: Started: ' + str(datetime.now()))

  # get our nodeset for this job
  scr_nodelist = scr_env.get_scr_nodelist()
  if scr_nodelist is None:
    scr_nodelist = scr_env.resmgr.get_job_nodes()
    if scr_nodelist is None:
      print('scr_postrun: ERROR: Could not identify nodeset')
      return 1
    os.environ['SCR_NODELIST'] = scr_nodelist

  # identify what nodes are still up
  upnodes = ','.join(scr_env.resmgr.expand_hosts(scr_nodelist))
  downnodes = list_down_nodes(nodeset=upnodes, scr_env=scr_env)

  if verbose:
    print('scr_postrun: upnodes:   ' + str(upnodes))
    print('scr_postrun: downnodes: ' + str(downnodes))

  # list_down_nodes returns 0 for none, 1 for error
  if type(downnodes) is int:
    downnodes = ''
    #if downnodes==1: # returned error
    #  return 1 # should we return an error here (?)
    #else: #returned 0, there were no down nodes
    #  downnodes = ''
  # otherwise a comma separated string of downnodes was returned
  else:
    # subtract the downnodes from the upnodes
    upnodes = scr_glob_hosts(minus=upnodes + ':' + downnodes,
                             resmgr=scr_env.resmgr)
  print('scr_postrun: UPNODES:   ' + upnodes)

  # if there is at least one remaining up node, attempt to scavenge
  ret = 1
  ### TODO : the default return value is 1, indicating failure.
  ### Should this be changed to zero, and conditionally set the return value to 1 ?
  ### This has no affect on anything else, as this is called as the script is ending.
  if upnodes != '':
    cntldir = list_dir(runcmd='control', scr_env=scr_env, bindir=bindir)
    # list_dir returns 1 or a space separated list of directories
    if cntldir == 1:
      print('scr_postrun: Unable to determine a control directory')
      return 1

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

    # track the id of the first one we fail to get
    # this will be used to list dsets --before "failed_dataset"
    # if there is no failure, we pass --before "0"
    failed_dataset = '0'

    # scavenge all output sets in ascending order
    print('scr_postrun: Looking for output sets')
    dsets_output = scr_flush_file.list_dsets_output()
    if not dsets_output:
      print('scr_postrun: Found no output set to scavenge')
    else:
      for d in dsets_output:
        # determine whether this dataset needs to be flushed
        if not scr_flush_file.need_flush(d):
          # dataset has already been flushed, go to the next one
          print('scr_postrun: Dataset ' + d + ' has already been flushed')
          continue
        print('scr_postrun: Attempting to scavenge dataset ' + d)

        # add $d to ATTEMPTED list
        attempted.append(d)

        # get dataset name
        dsetname = scr_flush_file.name(d)
        if not dsetname:
          # got a dataset to flush, but failed to get name
          print('scr_postrun: Failed to read name of dataset ' + d)
          failed_dataset = d
          break

        # build full path to dataset directory
        datadir = scr_env.dir_dset(d)
        os.makedirs(datadir, exist_ok=True)

        # Gather files from cache to parallel file system
        print('scr_postrun: Scavenging files from cache for ' + dsetname +
              ' to ' + datadir)
        if scr_scavenge(nodeset_job=scr_nodelist,
                        nodeset_up=upnodes,
                        dataset_id=d,
                        cntldir=cntldir,
                        prefixdir=pardir,
                        verbose=verbose,
                        scr_env=scr_env,
                        log=log) != 1:
          print('scr_postrun: Done scavenging files from cache for ' +
                dsetname + ' to ' + datadir)
        else:
          print('scr_postrun: ERROR: Scavenge files from cache for ' +
                dsetname + ' to ' + datadir)

        # check that gathered set is complete,
        # if not, don't update current marker
        #update_current=1
        print('scr_postrun: Checking that dataset is complete')
        if not scr_index.build(d):
          # failed to get dataset, stop trying for later sets
          failed_dataset = d
          break

        # remember that we scavenged this dataset in case we try again below
        succeeded.append(d)
        print('scr_postrun: Scavenged dataset ' + dsetname + ' successfully')

    # check whether we have a dataset set to flush
    print('scr_postrun: Looking for most recent checkpoint')
    dsets_ckpt = scr_flush_file.list_dsets_ckpt(before=failed_dataset)
    if not dsets_ckpt:
      print('scr_postrun: Found no checkpoint to scavenge')
    else:
      for d in dsets_ckpt:
        if d in attempted:
          if d in succeeded:
            # already got this one above, update current, and finish
            dsetname = scr_flush_file.name(d)
            if dsetname:
              print('scr_postrun: Already scavenged checkpoint dataset ' + d)
              print('scr_postrun: Updating current marker in index to ' +
                    dsetname)
              scr_index.current(dsetname)
              ret = 0
              break
          else:
            # already tried and failed, skip this dataset
            print('scr_postrun: Skipping checkpoint dataset ' + d +
                  ', since already failed to scavenge')
            continue

        # we have a dataset, check whether it still needs to be flushed

        if not scr_flush_file.need_flush(d):
          # found a dataset that has already been flushed, we can quit
          print('scr_postrun: Checkpoint dataset ' + d +
                ' has already been flushed')
          ret = 0
          break
        print('scr_postrun: Attempting to scavenge checkpoint dataset ' + d)

        # get dataset name
        dsetname = scr_flush_file.name(d)
        if not dsetname:
          # got a dataset to flush, but failed to get name
          print('scr_postrun: Failed to read name of checkpoint dataset ' + d)
          continue

        # build full path to dataset directory
        datadir = scr_env.dir_dset(d)
        os.makedirs(datadir, exist_ok=True)

        # Gather files from cache to parallel file system
        print('scr_postrun: Scavenging files from cache for checkpoint ' +
              dsetname + ' to ' + datadir)
        if scr_scavenge(nodeset_job=scr_nodelist,
                        nodeset_up=upnodes,
                        dataset_id=d,
                        cntldir=cntldir,
                        prefixdir=pardir,
                        verbose=verbose,
                        scr_env=scr_env,
                        log=log) != 1:
          print('scr_postrun: Done scavenging files from cache for ' +
                dsetname + ' to ' + datadir)
        else:
          print('scr_postrun: ERROR: Scavenge files from cache for ' +
                dsetname + ' to ' + datadir)

        # check that gathered set is complete,
        print('scr_postrun: Checking that dataset is complete')
        if scr_index.build(d):
          # make the new current
          # just completed scavenging this dataset, so quit
          print('scr_postrun: Updating current marker in index to ' + dsetname)
          scr_index.current(dsetname)
          ret = 0
          break

  # print the timing info
  end_time = datetime.now()
  end_secs = int(time())
  run_secs = end_secs - start_secs
  print('scr_postrun: Ended: ' + str(end_time))
  print('scr_postrun: secs: ' + str(run_secs))

  # print the exit code and exit
  print('scr_postrun: exit code: ' + str(ret))
  return ret
