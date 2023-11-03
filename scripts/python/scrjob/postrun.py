import os
from datetime import datetime
from time import time

from scrjob import hostlist
from scrjob.scavenge import scavenge
from scrjob.list_down_nodes import list_down_nodes
from scrjob.cli import SCRIndex, SCRFlushFile


def postrun(jobenv, verbose=False, log=None):
    """Called after all runs have completed in an allocation.

    This determines whether there are any datasets in cache that neeed
    to be copied to the prefix directory. It identifies any down nodes,
    and executes scavenge operations as needed.

    This iterates over all output datasets from oldest to newest,
    fetching each one if needed. If it fails to copy an output dataset,
    it notes that dataset id and stops.

    It then iterates over checkpoints from newest to oldest, excluding
    any checkpoint that comes after the first output dataset that it
    failed to copy, if any. It stops after it has ensured the most
    recent checkpoint is copied, given the above constraint.

    The point here is that if we fail to copy an output dataset, we
    ensure the job restarts from its most recent checkpoint before that
    output dataset so that the job will regenerate the missing output
    dataset when it runs again in the future.
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

    # get access to the index and flush files
    prefix = jobenv.dir_prefix()
    scr_index_file = SCRIndex(prefix)
    scr_flush_file = SCRFlushFile(prefix)

    # get our nodeset for this job
    jobnodes = jobenv.node_list()
    if not jobnodes:
        jobnodes = jobenv.resmgr.job_nodes()
        if not jobnodes:
            raise RuntimeError(
                'scr_postrun: ERROR: Could not identify nodeset')

        # TODO: explain why we do this
        os.environ['SCR_NODELIST'] = hostlist.join_hosts(jobnodes)

    # identify list of down nodes
    downnodes = list_down_nodes(jobenv, nodes=jobnodes)

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
    cntldirs = jobenv.dir_control()
    cntldir = cntldirs[0]

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
            scavenge(jobenv, upnodes, d, cntldir, log, verbose)
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
                    scr_index_file.current(dsetname)
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
            scavenge(jobenv, upnodes, d, cntldir, log, verbose)
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
            scr_index_file.current(dsetname)

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
