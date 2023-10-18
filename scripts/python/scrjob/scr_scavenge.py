#! /usr/bin/env python3

# scr_scavenge.py
# scavenge checkpoint files from cache to PFS

import os, sys

if 'scrjob' not in sys.path:
    sys.path.insert(0, '/'.join(os.path.realpath(__file__).split('/')[:-2]))
    import scrjob

import argparse
from time import time
from scrjob import scr_const
from scrjob.scr_param import SCR_Param
from scrjob.scr_environment import SCR_Env
from scrjob.resmgrs import AutoResourceManager
from scrjob.launchers import AutoJobLauncher

# check for pdsh / (clustershell) errors in case any nodes should be retried


def scr_scavenge(nodeset_job=None,
                 nodeset_up='',
                 nodeset_down='',
                 dataset_id=None,
                 cntldir=None,
                 prefixdir=None,
                 verbose=False,
                 scr_env=None,
                 log=None):
    """This script is invoked to perform a scavenge operation

  scr_scavenge.py is a wrapper to gather values and arrange parameters needed for
  a scavenge operation.

  When ready, the scavenge parameters are passed to the Joblauncher class to perform
  the scavenge operation.

  The output of the scavenge operation is both written to file and printed to screen.

  This script returns 1 if a needed value or parameter could not be determined,
  and returns 0 otherwise
  """
    # check that we have a nodeset for the job and directories to read from / write to
    if nodeset_job is None or dataset_id is None or cntldir is None or prefixdir is None:
        return 1

    bindir = scr_const.X_BINDIR

    # TODO: need to be able to set these defaults via config settings somehow
    # for now just hardcode the values

    if scr_env is None:
        scr_env = SCR_Env(prefix=prefixdir)
    if scr_env.param is None:
        scr_env.param = SCR_Param()
    if scr_env.resmgr is None:
        scr_env.resmgr = AutoResourceManager()
    if scr_env.launcher is None:
        scr_env.launcher = AutoJobLauncher()
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
        print('scr_scavenge: ERROR: Could not determine jobid.')
        return 1

    # build the output filenames
    dset_dir = scr_env.dir_dset(dataset_id)
    output = os.path.join(dset_dir, 'scr_scavenge.pdsh.o' + jobid)
    error = os.path.join(dset_dir, 'scr_scavenge.pdsh.e' + jobid)

    if verbose:
        print('scr_scavenge: nodeset_up =   ' + nodeset_up)
        print('scr_scavenge: nodeset_down = ' + nodeset_down)

    # format up and down nodesets for the scavenge command
    nodeset_up, nodeset_down = scr_env.resmgr.scavenge_nodelists(
        upnodes=nodeset_up, downnodes=nodeset_down)

    if verbose:
        print('scr_scavenge: upnodes =          ' + nodeset_up)
        print('scr_scavenge: downnodes_spaced = ' + nodeset_down)

    # log the start of the scavenge operation
    if log:
        log.event('SCAVENGE_START', dset=dataset_id)

    print('scr_scavenge: ' + str(int(time())))
    # have the launcher class gather files via pdsh or clustershell
    consoleout = scr_env.launcher.scavenge_files(prog=bindir + '/scr_copy',
                                                 upnodes=nodeset_up,
                                                 downnodes_spaced=nodeset_down,
                                                 cntldir=cntldir,
                                                 dataset_id=dataset_id,
                                                 prefixdir=prefixdir,
                                                 buf_size=buf_size,
                                                 crc_flag=crc_flag)

    # print outputs to screen
    try:
        os.makedirs('/'.join(output.split('/')[:-1]), exist_ok=True)
        with open(output, 'w') as outfile:
            outfile.write(consoleout[0])
        if verbose:
            print('scr_scavenge: stdout: cat ' + output)
            print(consoleout[0])
    except Exception as e:
        print(str(e))
        print('scr_scavenge: ERROR: Unable to write stdout to \"' + output +
              '\"')
    try:
        with open(error, 'w') as outfile:
            outfile.write(consoleout[1])
        if verbose:
            print('scr_scavenge: stderr: cat ' + error)
            print(consoleout[1])
    except Exception as e:
        print(str(e))
        print('scr_scavenge: ERROR: Unable to write stderr to \"' + error +
              '\"')

    # TODO: if we knew the total bytes, we could register a transfer here in addition to an event
    # get a timestamp for logging timing values
    end_time = int(time())
    diff_time = end_time - start_time
    if log:
        log.event('SCAVENGE_END', dset=dataset_id, secs=diff_time)

    return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser(add_help=False,
                                     argument_default=argparse.SUPPRESS,
                                     prog='scr_scavenge')
    parser.add_argument('-h',
                        '--help',
                        action='store_true',
                        help='Show this help message and exit.')
    parser.add_argument('-v',
                        '--verbose',
                        action='store_true',
                        default=False,
                        help='Verbose output.')
    parser.add_argument('-j',
                        '--jobset',
                        metavar='<nodeset>',
                        type=str,
                        default=None,
                        help='Specify the nodeset.')
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
    parser.add_argument('-i',
                        '--id',
                        metavar='<id>',
                        type=str,
                        default=None,
                        help='Specify the dataset id.')
    parser.add_argument('-f',
                        '--from',
                        metavar='<dir>',
                        type=str,
                        default=None,
                        help='The control directory.')
    parser.add_argument('-t',
                        '--to',
                        metavar='<dir>',
                        type=str,
                        default=None,
                        help='The prefix directory.')
    args = vars(parser.parse_args())
    if 'help' in args:
        parser.print_help()
    elif args['jobset'] is None or args['id'] is None or args[
            'from'] is None or args['to'] is None:
        parser.print_help()
        print('Required arguments: --jobset --id --from --to')
    else:
        ret = scr_scavenge(nodeset_job=args['jobset'],
                           nodeset_up=args['up'],
                           nodeset_down=args['down'],
                           dataset_id=args['id'],
                           cntldir=args['from'],
                           prefixdir=args['to'],
                           verbose=args['verbose'],
                           scr_env=None)
        print('scr_scavenge returned ' + str(ret))
