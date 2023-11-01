#! /usr/bin/env python3

import argparse

from scrjob import hostlist

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    # default=None, required=True, nargs='+'
    parser.add_argument('-c',
                        '--count',
                        action='store_true',
                        help='Print the number of hosts.')
    parser.add_argument('-n',
                        '--nth',
                        metavar='<num>',
                        type=int,
                        default=None,
                        help='Output the Nth host (1=lo, -1=hi).')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-h',
                       '--hosts',
                       metavar='<hosts>',
                       type=str,
                       default=None,
                       help='Use this hostlist.')
    group.add_argument('-m',
                       '--minus',
                       metavar='<s1:s2>',
                       type=str,
                       default=None,
                       help='Elements of s1 not in s2.')
    group.add_argument('-i',
                       '--intersection',
                       metavar='<s1:s2>',
                       type=str,
                       default=None,
                       help='Intersection of s1 and s2 hosts.')
    group.add_argument('-C',
                       '--compress',
                       metavar='<csv host>',
                       type=str,
                       default=None,
                       help='Compress the csv hostlist.')

    args = parser.parse_args()

    hosts = []

    if args.hosts:
        hoststr = args.hosts
    elif args.minus:
        hoststr = hostlist.glob_minus(args.minus)
    elif args.intersection:
        hoststr = hostlist.glob_intersection(args.intersection)
    elif args.compress:
        hoststr = args.compress
    else:
        raise RuntimeError('Hostlist not specified')

    if args.nth:
        print(hostlist.glob_nth(hoststr, args.nth))
    elif args.count:
        print(hostlist.glob_count(hoststr))
    elif args.compress:
        print(hostlist.glob_compress(hoststr))

    print(hoststr)
