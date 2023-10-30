# scavenge checkpoint files from cache to PFS

import os
from time import time

from scrjob import config


def scavenge(nodes_job=None,
             nodes_up=[],
             nodes_down=[],
             dataset_id=None,
             cntldir=None,
             prefixdir=None,
             verbose=False,
             jobenv=None,
             log=None):
    """Scavenge files for a dataset id from cache to the prefix directory.

    Arguments
    ---------
    nodes_job          list of nodes in the allocation.
    nodes_up           list of up nodes.
    nodes_down         list of down nodes.
    dataset_id         string, the dataset id.
    cntldir            string, the control directory path, obtained from Param.
    prefixdir          string, the prefix directory path.
    """

    # check that we have a nodeset for the job and directories to read from / write to
    if nodes_job is None or dataset_id is None or cntldir is None or prefixdir is None:
        raise RuntimeError(
            'scavenge: ERROR: nodeset, id, cntldir, or prefix not specified')

    # TODO: need to be able to set these defaults via config settings somehow
    # for now just hardcode the values

    # lookup buffer size and crc flag via scr_param
    param = jobenv.param

    buf_size = os.environ.get('SCR_FILE_BUF_SIZE')
    if buf_size is None:
        buf_size = str(1024 * 1024)

    # enable CRC on flush by default
    crc_flag = '--crc'
    crc_val = os.environ.get('SCR_CRC_ON_FLUSH')
    if crc_val == '0':
        crc_flag = None

    start_time = int(time())

    # tag output files with jobid
    jobid = jobenv.resmgr.job_id()
    if jobid is None:
        raise RuntimeError('scavenge: ERROR: Could not determine jobid.')

    # build the output filenames
    dset_dir = jobenv.dir_dset(dataset_id)
    out_file = os.path.join(dset_dir, 'scavenge.pdsh.o' + jobid)
    err_file = os.path.join(dset_dir, 'scavenge.pdsh.e' + jobid)

    # log the start of the scavenge operation
    if log:
        log.event('SCAVENGE_START', dset=dataset_id)

    if verbose:
        print('scavenge: nodes_up =   ' + str(nodes_up))
        print('scavenge: nodes_down = ' + str(nodes_down))
        print('scavenge: ' + str(int(time())))

    # have the launcher class gather files via pdsh or clustershell
    copy_exe = os.path.join(config.X_LIBEXECDIR, 'scr_copy')
    argv = [
        copy_exe,
        '--id',
        str(dataset_id),
        '--cntldir',
        cntldir,
        '--prefix',
        prefixdir,
        '--buf',
        buf_size,
    ]
    if crc_flag:
        argv.append(crc_flag)
    if nodes_down:
        downstr = ' '.join(nodes_down)
        argv.append(downstr)
    consoleout = jobenv.rexec.rexec(argv, nodes_up, jobenv)[0]

    # print output to screen
    try:
        os.makedirs('/'.join(out_file.split('/')[:-1]), exist_ok=True)
        with open(out_file, 'w') as f:
            f.write(consoleout[0])
        with open(err_file, 'w') as f:
            f.write(consoleout[1])
        if verbose:
            print('scavenge: stdout: cat ' + out_file)
            print(consoleout[0])
        if verbose:
            print('scavenge: stderr: cat ' + err_file)
            print(consoleout[1])
    except Exception as e:
        print(e)

    # TODO: if we knew the total bytes, we could register a transfer here in addition to an event
    # get a timestamp for logging timing values
    end_time = int(time())
    diff_time = end_time - start_time
    if log:
        log.event('SCAVENGE_END', dset=dataset_id, secs=diff_time)
