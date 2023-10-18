#! /usr/bin/env python3

# scr_halt.py

import os, sys

if 'scrjob' not in sys.path:
    sys.path.insert(0, '/'.join(os.path.realpath(__file__).split('/')[:-2]))
    import scrjob

import argparse
import stat
from scrjob import scr_const
from scrjob.scr_common import runproc
from scrjob.parsetime import parsetime


def scr_halt(bindir=None,
             checkpoints=None,
             before=None,
             after=None,
             immediate=False,
             seconds=None,
             dolist=False,
             unset_checkpoints=False,
             unset_before=False,
             unset_after=False,
             unset_seconds=False,
             unset_reason=False,
             remove=False,
             verbose=False,
             dirs=[]):
    ret = 0

    if bindir is None:
        bindir = scr_const.X_BINDIR

    # path to scr_halt_cntl command
    scr_halt_cntl = os.path.join(bindir, 'scr_halt_cntl')

    # use current working directory if none specified
    if not dirs:
        dirs = [os.getcwd()]

    # list to accumulate options for scr_halt_cntl command
    halt_conditions = []

    # the -r option overrides everything else
    if not remove:
        # halt after X checkpoints
        # TODO: check that a valid value was given
        if checkpoints is not None:
            checkpoints = str(checkpoints)
            halt_conditions = ['-c', checkpoints]

        # halt before time
        if before is not None:
            secs = parsetime(before)
            halt_conditions.append('-b')
            halt_conditions.append(str(secs))

        # halt after time
        if after is not None:
            secs = parsetime(after)
            halt_conditions.append('-a')
            halt_conditions.append(str(secs))

        # set (reset) SCR_HALT_SECONDS value
        # TODO: check that a valid value was given
        if seconds is not None:
            halt_conditions.append('-s')
            halt_conditions.append(seconds)

        # list halt options
        if dolist:
            halt_conditions.append('-l')

        # push options to unset any values
        if unset_checkpoints:
            halt_conditions.append('-xc')
        if unset_before:
            halt_conditions.append('-xb')
        if unset_after:
            halt_conditions.append('-xa')
        if unset_seconds:
            halt_conditions.append('-xs')
        if unset_reason:
            halt_conditions.append('-xr')

        # if we were not given any conditions, set the exit reason to JOB_HALTED
        if len(halt_conditions) == 0 or immediate:
            halt_conditions.append('-r')
            halt_conditions.append('JOB_HALTED')

    # create a halt file in each target prefix directory
    for d in dirs:
        print('Updating halt file in ' + d)
        rc = 0

        # build the name of the halt file
        halt_file = os.path.join(d, '.scr', 'halt.scr')

        # remove the halt file and move on to next direcotry
        # if no conditions are specified
        if halt_conditions == []:
            try:
                os.remove(halt_file)
            except:
                pass
            continue

        # create scr prefix directory
        os.makedirs(os.path.join(d, '.scr'), exist_ok=True)

        # execute the command
        # create the halt file with specified conditions
        # TODO: Set halt file permissions so system admins can modify them
        ####### ensure we have chmod664 ? then also need to ensure owned path is 775 ?
        ### If a chmod is good, 664+775 or 660+770
        ### Doing directories ... when to stop ? 2 dirs ?
        ### the makedirs is supposed to take a 'mode' argument, that didn't work for me.
        chmod664 = stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH
        chmod775 = stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | \
                   stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP | \
                   stat.S_IROTH | stat.S_IXOTH
        chmod660 = stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP
        chmod770 = stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | \
                   stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP
        chmod664 = None

        cmd = [scr_halt_cntl, '-f', halt_file]
        cmd.extend(halt_conditions)
        output, rc = runproc(cmd,
                             getstdout=True,
                             getstderr=True,
                             verbose=verbose)
        if rc != 0:
            if output is not None:
                print(output[1].strip())
            print('scr_halt: ERROR: Failed to update halt file for ' + d)
            ret = 1
        # TODO For file permissions ?
        # Doing 664+775
        #else: # rc == 0:
        elif chmod664 is not None:
            # the file
            try:
                # 664 OR with whatever was there before
                os.chmod(halt_file, chmod664 | os.stat(halt_file).st_mode)
            except:
                pass
            chdir = '/'.join(halt_file.split('/')[:-1])
            try:
                # do both directories: d / .scr
                os.chmod(chdir, chmod775 | os.stat(chdir).st_mode)
                chdir = '/'.join(chdir.split('/')[:-1])
                os.chmod(chdir, chmod775 | os.stat(chdir).st_mode)
            except:
                pass

        # print output to screen
        if output is not None:
            print(output[0].strip())

    # kill job if immediate was set
    # TODO: would like to protect against killing a job in the middle of a checkpoint if possible
    if immediate:
        # TODO: lookup active jobid for given prefix directory and halt job based on system
        print('scr_halt: ERROR: --immediate option not yet supported')
        return 1

    return ret


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        add_help=False,
        argument_default=argparse.SUPPRESS,
        prog='scr_halt',
        epilog=
        'TIME arguments are parsed using parsetime.py,\nand t may be specified in one of many formats.\nExamples include \'12pm\', \'yesterday noon\', \'12/25 15:30:33\', and so on.\nIf no directory is specified, the current working directory is used.'
    )
    # when prefixes are unambiguous then also adding shortcodes isn't necessary
    #parser.add_argument('-z', '--all', action='store_true', help='Halt all jobs on the system.')
    #parser.add_argument('-u', '--user', metavar='LIST', type=str, help='Halt all jobs for a comma-separated LIST of users.')
    parser.add_argument('-c',
                        '--checkpoints',
                        metavar='N',
                        default=None,
                        type=int,
                        help='Halt job after N checkpoints.')
    parser.add_argument(
        '-b',
        '--before',
        metavar='TIME',
        default=None,
        type=str,
        help='Halt job before specified TIME. Uses SCR_HALT_SECONDS if set.')
    parser.add_argument('-a',
                        '--after',
                        metavar='TIME',
                        default=None,
                        type=str,
                        help='Halt job after specified TIME.')
    parser.add_argument('-i',
                        '--immediate',
                        action='store_true',
                        default=False,
                        help='Halt job immediately.')
    parser.add_argument('-s',
                        '--seconds',
                        metavar='N',
                        default=None,
                        type=str,
                        help='Set or reset SCR_HALT_SECONDS for active job.')
    parser.add_argument(
        '-l',
        '--list',
        action='store_true',
        default=False,
        help='List the current halt conditions specified for a job or jobs.')
    parser.add_argument('--unset-checkpoints',
                        action='store_true',
                        default=False,
                        help='Unset any checkpoint halt condition.')
    parser.add_argument('--unset-before',
                        action='store_true',
                        default=False,
                        help='Unset any halt before condition.')
    parser.add_argument('--unset-after',
                        action='store_true',
                        default=False,
                        help='Unset halt after condition.')
    parser.add_argument('--unset-seconds',
                        action='store_true',
                        default=False,
                        help='Unset halt seconds.')
    parser.add_argument('--unset-reason',
                        action='store_true',
                        default=False,
                        help='Unset the current halt reason.')
    parser.add_argument('-r',
                        '--remove',
                        action='store_true',
                        default=False,
                        help='Remove halt file.')
    parser.add_argument('-v',
                        '--verbose',
                        action='store_true',
                        default=False,
                        help='Increase verbosity.')
    parser.add_argument('-h',
                        '--help',
                        action='store_true',
                        help='Show this help message and exit.')
    parser.add_argument('dirs', nargs=argparse.REMAINDER, default=[])
    args = vars(parser.parse_args())

    if 'help' in args:
        parser.print_help()
    #elif 'all' in args or 'user' in args:
    #  print('scr_halt: ERROR: --all and --user options not yet supported')
    else:
        ret = scr_halt(checkpoints=args['checkpoints'],
                       before=args['before'],
                       after=args['after'],
                       immediate=args['immediate'],
                       seconds=args['seconds'],
                       dolist=args['list'],
                       unset_checkpoints=args['unset_checkpoints'],
                       unset_before=args['unset_before'],
                       unset_after=args['unset_after'],
                       unset_seconds=args['unset_seconds'],
                       unset_reason=args['unset_reason'],
                       remove=args['remove'],
                       verbose=args['verbose'],
                       dirs=args['dirs'])
        print(str(ret))
