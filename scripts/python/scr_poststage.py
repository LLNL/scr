# SCR allows you to spawn off dataset transfers "in the background"
# that will finish some time after a job completes.  This saves you from
# using your compute time to transfer datasets.  You can do this by
# specifying SCR_FLUSH_POSTAGE=1 in your SCR config.  This currently is
# only supported on on IBM burst buffer nodes, and only when FLUSH=BBAPI is
# specified in the burst buffer's storage descriptor.
#
# This script is to be run as a 2nd-half post-stage script on an IBM
# system.  A 2nd-half post-stage script will run after all the job's burst
# buffer transfers have finished (which could be hours later after the job
# finishes).  This script will finalize any completed burst buffer dataset
# transfers so that they're visible to SCR.
#

# Pass the prefix as an argument
# e.g., python3 scr_poststage.py /tmp
# or scr_poststage('/tmp')

import os
import sys
import argparse
from datetime import datetime

from scrjob.cli import SCRFlushFile, SCRIndex


# do_poststage is called from scr_poststage below
# not intended to be directly called
# can hardcode the logfile in scr_poststage
def do_poststage(prefix=None, logfile=None):
    # interface to query the SCR flush file
    scr_flush_file = SCRFlushFile(prefix)

    # interface to query the SCR index file
    scr_index = SCRIndex(prefix)

    logfile.write(str(datetime.now()) + ' Begin post_stage\n')

    logfile.write('Current index before finalizing:\n')
    out = scr_index.list()
    logfile.write(out)

    # If we fail to finalize any dataset, set this to the ID of that dataset.
    # We later then only attempt to finalize checkpoints up to the first
    # failed output dataset ID.
    failed_id = None

    logfile.write('--- Processing output datasets ---\n')
    dsets = scr_flush_file.list_dsets_output()
    for cid in dsets:
        # Get name of this dataset id
        dset = scr_flush_file.name(cid)
        logfile.write('Looking at output dataset ' + str(cid) + ' (' +
                      str(dset) + ')\n')

        if not scr_flush_file.need_flush(cid):
            # Dataset is already flushed, skip it
            logfile.write('Output dataset ' + str(cid) + ' (' + str(dset) +
                          ') is already flushed, skip it\n')
            continue

        logfile.write('Finalizing transfer for dataset ' + str(cid) + ' (' +
                      str(dset) + ')\n')
        if not scr_flush_file.resume(cid):
            logfile.write('Error: Can\'t resume output dataset ' + str(cid) +
                          ' (' + str(dset) + ')\n')
            failed_id = cid
            break

        logfile.write('Writing summary for dataset ' + str(cid) + ' (' +
                      str(dset) + ')\n')
        if not scr_flush_file.write_summary(cid):
            logfile.write('ERROR: can\'t write summary for output dataset ' +
                          str(cid) + ' (' + str(dset) + ')\n')
            failed_id = cid
            break

        logfile.write('Adding dataset ' + str(cid) + ' (' + str(dset) +
                      ') to index\n')
        if not scr_index.add(dset):
            logfile.write('Couldn\'t add output dataset ' + str(cid) + ' (' +
                          str(dset) + ') to index\n')
            failed_id = cid
            break

    # Finalize each checkpoint listed in the flush file.  If there are any
    # failed output files (FAILED_ID > 0) then only finalize checkpoints
    # up to the last good output file.  If there are no failures
    # (FAILED_ID = 0) then all checkpoints are iterated over.
    logfile.write('--- Processing checkpoints ---\n')
    dsets = scr_flush_file.list_dsets_ckpt(before=failed_id)
    for cid in dsets:
        # Get name of this dataset id
        dset = scr_flush_file.name(cid)
        logfile.write('Looking at checkpoint dataset ' + str(cid) + ' (' +
                      str(dset) + ')\n')

        if not scr_flush_file.need_flush(cid):
            # Dataset is already flushed, skip it
            logfile.write('Checkpoint dataset ' + str(cid) + ' (' + str(dset) +
                          ') is already flushed, skip it\n')
            continue

        logfile.write('Finalizing transfer for checkpoint dataset ' +
                      str(cid) + ' (' + str(dset) + ')\n')
        if not scr_flush_file.resume(cid):
            logfile.write('Error: Can\'t resume checkpoint dataset ' +
                          str(cid) + ' (' + str(dset) + ')\n')
            continue

        logfile.write('Writing summary for checkpoint dataset ' + str(cid) +
                      ' (' + str(dset) + ')\n')
        if not scr_flush_file.write_summary(cid):
            logfile.write(
                'ERROR: can\'t write summary for checkpoint dataset ' +
                str(cid) + ' (' + str(dset) + ')\n')
            continue

        logfile.write('Adding checkpoint dataset ' + str(cid) + ' (' +
                      str(dset) + ') to index\n')
        if not scr_index.add(dset):
            logfile.write('Couldn\'t add checkpoint dataset ' + str(cid) +
                          ' (' + str(dset) + ') to index\n')
            continue

        logfile.write('Setting current checkpoint dataset to ' + str(cid) +
                      ' (' + str(dset) + ')\n')
        if not scr_index.current(dset):
            logfile.write('Couldn\'t set current checkpoint dataset to ' +
                          str(cid) + ' (' + str(dset) + ')\n')

    logfile.write('All done, index now:\n')
    out = scr_index.list()
    logfile.write(out)


def scr_poststage(prefix=None):
    if prefix is None:
        return

    # Path to where you want the scr_poststage log
    logfile = os.path.join(prefix, 'scr_poststage.log')
    try:
        os.makedirs('/'.join(logfile.split('/')[:-1]), exist_ok=True)
        with open(logfile, 'a') as logfile:
            do_poststage(prefix, logfile)
    except:
        pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser(add_help=False,
                                     argument_default=argparse.SUPPRESS,
                                     prog='scr_poststage')
    parser.add_argument('-h',
                        '--help',
                        action='store_true',
                        help='Show this help message and exit.')
    parser.add_argument('-p',
                        '--prefix',
                        metavar='<dir>',
                        type=str,
                        default=None,
                        help='Specify the prefix directory.')
    parser.add_argument('rest', nargs=argparse.REMAINDER)
    args = vars(parser.parse_args())
    if 'help' in args:
        parser.print_help()
    elif args['prefix'] is None and (args['rest'] is None
                                     or len(args['rest']) == 0):
        print('The prefix directory must be specified.')
    elif args['prefix'] is None:
        scr_poststage(prefix=args['rest'][0])
    else:
        scr_poststage(prefix=args['prefix'])
