#! /usr/bin/env python3

# scavenge checkpoint files from cache to PFS

import os
import sys
import argparse
from time import time

from scrjob import scr_const
from scrjob.environment import SCR_Env

# check for pdsh / (clustershell) errors in case any nodes should be retried


def scr_scavenge(nodes_job=None,
                 nodes_up=[],
                 nodes_down=[],
                 dataset_id=None,
                 cntldir=None,
                 prefixdir=None,
                 verbose=False,
                 scr_env=None,
                 log=None):
    # check that we have a nodeset for the job and directories to read from / write to
    if nodes_job is None or dataset_id is None or cntldir is None or prefixdir is None:
        raise RuntimeError(
            'scr_scavenge: ERROR: nodeset, id, cntldir, or prefix not specified'
        )

    libexecdir = scr_const.X_LIBEXECDIR

    # TODO: need to be able to set these defaults via config settings somehow
    # for now just hardcode the values

    # lookup buffer size and crc flag via scr_param
    param = scr_env.param

    buf_size = os.environ.get('SCR_FILE_BUF_SIZE')
    if buf_size is None:
        buf_size = str(1024 * 1024)

    crc_flag = os.environ.get('SCR_CRC_ON_FLUSH')
    if crc_flag is None:
        crc_flag = '--crc'
    elif crc_flag == '0':
        crc_flag = ''

    start_time = int(time())

    # tag output files with jobid
    jobid = scr_env.resmgr.job_id()
    if jobid is None:
        raise RuntimeError('scr_scavenge: ERROR: Could not determine jobid.')

    # build the output filenames
    dset_dir = scr_env.dir_dset(dataset_id)
    out_file = os.path.join(dset_dir, 'scr_scavenge.pdsh.o' + jobid)
    err_file = os.path.join(dset_dir, 'scr_scavenge.pdsh.e' + jobid)

    # log the start of the scavenge operation
    if log:
        log.event('SCAVENGE_START', dset=dataset_id)

    if verbose:
        print('scr_scavenge: nodes_up =   ' + str(nodes_up))
        print('scr_scavenge: nodes_down = ' + str(nodes_down))
        print('scr_scavenge: ' + str(int(time())))

    # have the launcher class gather files via pdsh or clustershell
    copy_exe = os.path.join(libexecdir, 'scr_copy')
    consoleout = scr_env.launcher.scavenge_files(prog=copy_exe,
                                                 nodes_up=nodes_up,
                                                 nodes_down=nodes_down,
                                                 cntldir=cntldir,
                                                 dataset_id=dataset_id,
                                                 prefixdir=prefixdir,
                                                 buf_size=buf_size,
                                                 crc_flag=crc_flag)

    # print output to screen
    try:
        os.makedirs('/'.join(out_file.split('/')[:-1]), exist_ok=True)
        with open(out_file, 'w') as f:
            f.write(consoleout[0])
        with open(err_file, 'w') as f:
            f.write(consoleout[1])
        if verbose:
            print('scr_scavenge: stdout: cat ' + out_file)
            print(consoleout[0])
        if verbose:
            print('scr_scavenge: stderr: cat ' + err_file)
            print(consoleout[1])
    except Exception as e:
        print(e)

    # TODO: if we knew the total bytes, we could register a transfer here in addition to an event
    # get a timestamp for logging timing values
    end_time = int(time())
    diff_time = end_time - start_time
    if log:
        log.event('SCAVENGE_END', dset=dataset_id, secs=diff_time)


if __name__ == '__main__':
    """This script is invoked to perform a scavenge operation.

    scr_scavenge.py is a wrapper to gather values and arrange parameters
    needed for a scavenge operation.

    When ready, the scavenge parameters are passed to the Joblauncher
    class to perform the scavenge operation.

    The output of the scavenge operation is both written to file and
    printed to screen.

    Exits with 1 on error, 0 on success.
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('-j',
                        '--jobset',
                        metavar='<nodeset>',
                        type=str,
                        default=None,
                        required=True,
                        help='Specify the nodeset.')
    parser.add_argument('-i',
                        '--id',
                        metavar='<id>',
                        type=str,
                        default=None,
                        required=True,
                        help='Specify the dataset id.')
    parser.add_argument('-f',
                        '--from',
                        metavar='<dir>',
                        type=str,
                        default=None,
                        required=True,
                        help='The control directory.')
    parser.add_argument('-t',
                        '--to',
                        metavar='<dir>',
                        type=str,
                        default=None,
                        required=True,
                        help='The prefix directory.')
    parser.add_argument('-u',
                        '--up',
                        metavar='<nodeset>',
                        type=str,
                        default=None,
                        help='Specify up nodes.')
    parser.add_argument('-d',
                        '--down',
                        metavar='<nodeset>',
                        type=str,
                        default=None,
                        help='Specify down nodes.')
    parser.add_argument('-v',
                        '--verbose',
                        action='store_true',
                        default=False,
                        help='Verbose output.')

    args = parser.parse_args()

    scr_env = SCR_Env(prefix=None)

    nodes_job = scr_env.resmgr.expand_hosts(args.jobset)
    nodes_up = scr_env.resmgr.expand_hosts(args.up)
    nodes_down = scr_env.resmgr.expand_hosts(args.down)

    scr_scavenge(nodes_job=nodes_job,
                 nodes_up=nodes_up,
                 nodes_down=nodes_down,
                 dataset_id=args.id,
                 cntldir=args['from'],
                 prefixdir=args.to,
                 verbose=args.verbose,
                 scr_env=None)
