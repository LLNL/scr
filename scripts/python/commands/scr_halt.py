#! /usr/bin/env python3

# add path holding scrjob to PYTHONPATH
import sys

sys.path.insert(0, '@X_LIBEXECDIR@/python')

import os
import re
import datetime
import argparse

from scrjob.cli.scr_halt_cntl import SCRHaltFile


def parsetime(timestr):
    re_ts_full = re.compile(r'(\d\d\d\d)-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d)$')
    m = re_ts_full.match(timestr)
    if m:
        year  = int(m.groups(0)[0])
        month = int(m.groups(0)[1])
        day   = int(m.groups(0)[2])
        hours = int(m.groups(0)[3])
        mins  = int(m.groups(0)[4])
        secs  = int(m.groups(0)[5])
        dt = datetime.datetime(year=year, month=month, day=day, hour=hours, minute=mins, second=secs)
        return int(dt.timestamp())

    #re_ts_24hr = re.compile(r'^(\d\d):(\d\d):(\d\d)$')
    #m = re_ts_24hr.match(timestr)
    #if m:
    #    hours = int(m.groups(0)[0])
    #    mins  = int(m.groups(0)[1])
    #    secs  = int(m.groups(0)[2])
    #    dt = datetime.datetime.now()
    #    dt = dt.replace(hour=hours, minute=mins, second=secs)
    #    return int(dt.timestamp())

    raise RuntimeError(f"Unsupported time format: '{timestr}'")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        epilog=
        'TIME arguments must be ISO format YYYY-MM-DDTHH:MM:SS in the current time zone.\nIf no directory is specified, the current working directory is used.'
    )

    # when prefixes are unambiguous then also adding shortcodes isn't necessary
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
                        type=int,
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
    parser.add_argument('dirs', nargs=argparse.REMAINDER, default=[])

    args = parser.parse_args()

    dirs = args.dirs if args.dirs else [os.getcwd()]
    for d in dirs:
        halt = SCRHaltFile(d, verbose=args.verbose)

        if args.remove:
            halt.remove()
            continue

        if args.list:
            halt.set_list()

        if args.checkpoints:
            halt.set_checkpoints(args.checkpoints)

        if args.before:
            secs = parsetime(args.before)
            halt.set_before(secs)

        if args.after:
            secs = parsetime(args.after)
            halt.set_after(secs)

        if args.seconds:
            halt.set_seconds(args.seconds)

        if args.unset_checkpoints:
            halt.unset_checkpoints()

        if args.unset_before:
            halt.unset_before()

        if args.unset_after:
            halt.unset_after()

        if args.unset_seconds:
            halt.unset_seconds()

        if args.unset_reason:
            halt.unset_reason()

        result = halt.execute()
        if result:
            print(result)
