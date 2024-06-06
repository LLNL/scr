# scavenge dataset files from cache to prefix directory
# invokes scr_index to rebuild missing files if possible

import os
from datetime import datetime
from time import time

from scrjob import config
from scrjob.cli import SCRIndex, SCRFlushFile


def scavenge_files(jobenv,
                   nodes,
                   dataset_id,
                   cntldir,
                   nodes_down=None,
                   log=None,
                   verbose=False):
    """Copies user and redundancy files for a dataset id from cache to the
    prefix directory."""

    start_time = int(time())

    # get SCR prefix directory
    prefix = jobenv.dir_prefix()

    # TODO: need to be able to set these defaults via config settings somehow
    # for now just hardcode the values
    # lookup buffer size and crc flag via scr_param
    param = jobenv.param

    # used as buffer size when doing file copy operations from cache to prefix directory
    buf_size = os.environ.get('SCR_FILE_BUF_SIZE')
    if buf_size is None:
        buf_size = str(1024 * 1024)

    # enable CRC on flush by default
    # computes CRC when copying files, checks against CRC recorded for file if it exists
    # stores CRC with file on prefix directory for use when reading file back
    crc_flag = '--crc'
    crc_val = os.environ.get('SCR_CRC_ON_FLUSH')
    if crc_val == '0':
        crc_flag = None

    # tag output files with jobid
    jobid = jobenv.resmgr.job_id()
    if jobid is None:
        raise RuntimeError('Could not determine jobid.')

    # log the start of the scavenge operation
    if log:
        log.event('SCAVENGE_START', dset=dataset_id)

    if verbose:
        print('scavenge: start: ' + str(int(time())))
        print('scavenge: nodes: ' + str(nodes))

    # run command on each node to copy files from cache to prefix directory
    copy_exe = os.path.join(config.X_LIBEXECDIR, 'scr_copy')
    argv = [
        copy_exe,
        '--id',
        str(dataset_id),
        '--cntldir',
        cntldir,
        '--prefix',
        prefix,
        '--buf',
        buf_size,
    ]
    if crc_flag:
        argv.append(crc_flag)
    if nodes_down:
        argv.extend(nodes_down)
    result = jobenv.rexec.rexec(argv, nodes, jobenv)

    # build the output filenames
    dset_dir = jobenv.dir_dset(dataset_id)
    out_file = os.path.join(dset_dir, 'scavenge.o' + jobid)
    err_file = os.path.join(dset_dir, 'scavenge.e' + jobid)

    try:
        # write stdout and stderr to files
        os.makedirs('/'.join(out_file.split('/')[:-1]), exist_ok=True)

        with open(out_file, 'w') as f:
            for node in nodes:
                text = result.stdout(node)
                f.write(node + '\n')
                f.write(text + '\n')
                f.write("\n")
                if verbose:
                    print(node)
                    print(text)

        with open(err_file, 'w') as f:
            for node in nodes:
                text = result.stderr(node)
                f.write(node + '\n')
                f.write(text + '\n')
                f.write("\n")
                if verbose:
                    print(node)
                    print(text)

    except Exception as e:
        print(e)

    # TODO: if we knew the total bytes, we could register a transfer here in addition to an event
    # get a timestamp for logging timing values
    end_time = int(time())
    diff_time = end_time - start_time
    if log:
        log.event('SCAVENGE_END', dset=dataset_id, secs=diff_time)


def scavenge(jobenv, nodes, dataset_id, cntldir, log=None, verbose=False):
    """Copies dataset files from cache to prefix directory and attempts to
    rebuild."""

    # get access to the index and flush files for the job
    prefix = jobenv.dir_prefix()
    scr_index_file = SCRIndex(prefix)
    scr_flush_file = SCRFlushFile(prefix)

    # get dataset name
    dsetname = scr_flush_file.name(dataset_id)
    if not dsetname:
        raise RuntimeError('Failed to read name of dataset ' + dataset_id)

    # create dataset directory within prefix directory
    datadir = jobenv.dir_dset(dataset_id)
    os.makedirs(datadir, exist_ok=True)

    if verbose:
        print('scavenge: Scavenging files for dataset ' + dsetname + ' to ' +
              datadir)

    # Scavenge files from cache to parallel file system
    scavenge_files(jobenv,
                   nodes,
                   dataset_id,
                   cntldir,
                   log=log,
                   verbose=verbose)

    if verbose:
        print('scavenge: Checking files for dataset ' + dsetname)

    # rebuild missing files from redundancy scheme if possible
    if not scr_index_file.build(dataset_id):
        raise RuntimeError('Failed to rebuild dataset ' + dataset_id)

    if verbose:
        print('scavenge: Verified files for dataset ' + dsetname)
