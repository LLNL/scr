#! /usr/bin/env python3

import argparse

import scrjob.glob_hosts as glob_hosts

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
        hosts = glob_hosts.expand(args.hosts)
    elif args.minus:
        hosts = glob_hosts.minus(args.minus)
    elif args.intersection:
        hosts = glob_hosts.intersection(args.intersection)
    elif args.compress:
        hosts = glob_hosts.expand(args.compress)
    else:
        raise RuntimeError('Hostlist not specified')

    hoststr = ','.join(hosts)

    if args.nth:
        print(str(glob_hosts.nth(hoststr, args.nth)))
    elif args.count:
        print(str(glob_hosts.count(hoststr)))
    elif args.compress:
        print(str(glob_hosts.compress(hoststr)))

    print(str(result))
