#! /usr/bin/env python3

import os
from datetime import datetime

from scrjob import scr_const
from scrjob.list_dir import list_dir
from scrjob.scr_scavenge import scr_scavenge
from scrjob.list_down_nodes import list_down_nodes
from scrjob.cli import SCRIndex, SCRFlushFile


def scavenge_rebuild(nodes_job,
                     nodes_up,
                     d,
                     cntldir,
                     pardir,
                     scr_env,
                     log,
                     verbose=False):

    scr_index = SCRIndex(pardir)
    scr_flush_file = SCRFlushFile(pardir)

    # get dataset name
    dsetname = scr_flush_file.name(d)
    if not dsetname:
        raise RuntimeError('scr_postrun: Failed to read name of dataset ' + d)

    # create dataset directory within prefix directory
    datadir = scr_env.dir_dset(d)
    os.makedirs(datadir, exist_ok=True)

    # Scavenge files from cache to parallel file system
    if verbose:
        print('scr_postrun: Scavenging ' + dsetname + ' to ' + datadir)
    scr_scavenge(nodes_job=nodes_up,
                 nodes_up=nodes_up,
                 dataset_id=d,
                 cntldir=cntldir,
                 prefixdir=pardir,
                 verbose=verbose,
                 scr_env=scr_env,
                 log=log)

    # check that scavenged set of files is complete,
    # rebuilding missing files if possible
    if verbose:
        print('scr_postrun: Checking files for dataset ' + dsetname)
    if not scr_index.build(d):
        raise RuntimeError('scr_postrun: ERROR: failed to rebuild dataset ' +
                           d)
    if verbose:
        print('scr_postrun: Scavenged dataset ' + dsetname)


def postrun(prefix_dir=None, scr_env=None, verbose=False, log=None):
    """This method is called after all runs has completed, before scr_run.py
    exits.

    Determine whether there are datasets to scavenge, and perform
    scavenge operations
    """

    # if SCR is disabled, immediately exit
    val = os.environ.get('SCR_ENABLE')
    if val == '0':
        return

    # record the start time for timing purposes
    start_time = datetime.now()
    start_secs = int(time())
    if verbose:
        print('scr_postrun: Started: ' + str(datetime.now()))

    if scr_env is None or scr_env.resmgr is None:
        raise RuntimeError(
            'scr_postrun: ERROR: environment and/or resource manager not set')

    # check that we have the SCR_PREFIX directory
    pardir = prefix_dir
    if not pardir:
        pardir = scr_env.dir_prefix()
    if not pardir:
        raise RuntimeError(
            'scr_postrun: ERROR: SCR_PREFIX directory not specified')

    scr_index = SCRIndex(pardir)
    scr_flush_file = SCRFlushFile(pardir)

    # get our nodeset for this job
    scr_nodelist = scr_env.node_list()
    if not scr_nodelist:
        scr_nodelist = scr_env.resmgr.job_nodes()
        if not scr_nodelist:
            raise RuntimeError(
                'scr_postrun: ERROR: Could not identify nodeset')

        # TODO: explain why we do this
        os.environ['SCR_NODELIST'] = scr_env.resmgr.join_hosts(scr_nodelist)

    # identify list of down nodes
    jobnodes = scr_nodelist
    downnodes = list_down_nodes(nodes=jobnodes, scr_env=scr_env)

    # identify what nodes are still up
    upnodes = [n for n in jobnodes if n not in downnodes]

    if verbose:
        print('scr_postrun: jobnodes:  ' + str(jobnodes))
        print('scr_postrun: downnodes: ' + str(downnodes))
        print('scr_postrun: upnodes:   ' + str(upnodes))

    ### TODO : the default return value is 1, indicating failure.
    ### Should this be changed to zero, and conditionally set the return value to 1 ?
    ### This has no affect on anything else, as this is called as the script is ending.

    # need at least one remaining up node to attempt to scavenge
    if not upnodes:
        raise RuntimeError('scr_postrun: ERROR: No remaining upnodes')

    # get list of control directories
    cntldirs = list_dir(runcmd='control', scr_env=scr_env)
    cntldir = cntldirs[0]

    # TODODSET: avoid scavenging things unless it's in this list
    # get list of possible datasets
    #  dataset_list=`$bindir/scr_inspect --up $UPNODES --from $cntldir`
    #  if [ $? -eq 0 ] ; then
    #  else
    #    echo "$prog: Failed to inspect cache or cannot scavenge any datasets"
    #  fi

    # get list of all output sets in ascending order
    if verbose:
        print('scr_postrun: Looking for output sets')
    dsets_output = scr_flush_file.list_dsets_output()
    if not dsets_output and verbose:
        print('scr_postrun: Found no output set to scavenge')

    # track list of dataset ids we attempt and succeed to scavenge
    attempted = []
    succeeded = []

    # track the id of the first dataset we fail to get
    # this will be used to list dsets --before "failed_dataset"
    # if there is no failure, we pass --before "0"
    failed_dataset = '0'

    # scavenge all output sets in ascending order
    for d in dsets_output:
        # determine whether this dataset needs to be flushed
        if not scr_flush_file.need_flush(d):
            # dataset has already been flushed, go to the next one
            if verbose:
                print('scr_postrun: Dataset ' + d +
                      ' has already been flushed')
            continue

        try:
            attempted.append(d)
            scavenge_rebuild(scr_nodelist, upnodes, d, cntldir, pardir,
                             scr_env, log, verbose)
            succeeded.append(d)
        except Exception as e:
            if verbose:
                print(e)
                print('scr_postrun: Failed to scavenge dataset ' + d)
            failed_dataset = d
            break

    # get list of checkpoints to flush
    # if we failed to scavenge a dataset,
    # limit to checkpoints that precede that failed dataset
    if verbose:
        print('scr_postrun: Looking for checkpoints')
    dsets_ckpt = scr_flush_file.list_dsets_ckpt(before=failed_dataset)
    if not dsets_ckpt and verbose:
        print('scr_postrun: Found no checkpoint to scavenge')

    # scavenge a checkpoint
    found_ckpt = False
    for d in dsets_ckpt:
        # since output datasets can also be checkpoints,
        # skip any checkpoint that we already attemped above
        if d in attempted:
            if d in succeeded:
                # already got this one above, update current, and finish
                dsetname = scr_flush_file.name(d)
                if dsetname:
                    if verbose:
                        print(
                            'scr_postrun: Already scavenged checkpoint dataset '
                            + d)
                        print(
                            'scr_postrun: Updating current marker in index to '
                            + dsetname)
                    scr_index.current(dsetname)
                    found_ckpt = True
                    break
            else:
                # already tried and failed, skip this dataset
                if verbose:
                    print('scr_postrun: Skipping checkpoint dataset ' + d +
                          ', since already failed to scavenge')
                continue

        # found a checkpoint that we haven't already attempted,
        # check whether it still needs to be flushed
        if not scr_flush_file.need_flush(d):
            # found a dataset that has already been flushed, we can quit
            if verbose:
                print('scr_postrun: Checkpoint dataset ' + d +
                      ' has already been flushed')
            found_ckpt = True
            break

        # attempt to scavenge this checkpoint
        try:
            scavenge_rebuild(scr_nodelist, upnodes, d, cntldir, pardir,
                             scr_env, log, verbose)
        except Exception as e:
            # failed to scavenge and/or rebuild, attempt the next checkpoint
            if verbose:
                print(e)
                print('scr_postrun: Failed to scavenge checkpoint dataset ' +
                      d)
            continue

        # update current to point an newly scavenged checkpoint
        dsetname = scr_flush_file.name(d)
        if dsetname:
            if verbose:
                print('scr_postrun: Updating current marker in index to ' +
                      dsetname)
            scr_index.current(dsetname)

        # completed scavenging this checkpoint, so quit
        found_ckpt = True
        break

    # print the timing info
    end_time = datetime.now()
    end_secs = int(time())
    run_secs = end_secs - start_secs
    if verbose:
        print('scr_postrun: Ended: ' + str(end_time))
        print('scr_postrun: secs: ' + str(run_secs))
