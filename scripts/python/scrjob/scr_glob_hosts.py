#! /usr/bin/env python3

import os, sys
import sys
import argparse

from scrjob import scr_hostlist


def scr_glob_hosts(count=False,
                   nth=None,
                   hosts=None,
                   minus=None,
                   intersection=None,
                   compress=None,
                   resmgr=None):
    hostset = []

    # resmgr can use ClusterShell.NodeSet, if available
    # otherwise fallback to scr_hostlist

    if hosts is not None:
        if resmgr is not None:
            hostset = resmgr.expand_hosts(hostnames=hosts)
        else:
            hostset = scr_hostlist.expand(hosts)

    elif minus is not None and ':' in minus:
        # subtract nodes in set2 from set1
        pieces = minus.split(':')
        if resmgr is not None:
            set1 = resmgr.expand_hosts(pieces[0])
            set2 = resmgr.expand_hosts(pieces[1])
            hostset = resmgr.diff_hosts(set1, set2)
        else:
            set1 = scr_hostlist.expand(pieces[0])
            set2 = scr_hostlist.expand(pieces[1])
            hostset = scr_hostlist.diff(set1, set2)

    elif intersection is not None and ':' in intersection:
        # take the intersection of two nodesets
        pieces = intersection.split(':')
        if resmgr is not None:
            set1 = resmgr.expand_hosts(pieces[0])
            set2 = resmgr.expand_hosts(pieces[1])
            hostset = resmgr.intersect_hosts(set1, set2)
        else:
            set1 = scr_hostlist.expand(pieces[0])
            set2 = scr_hostlist.expand(pieces[1])
            hostset = scr_hostlist.intersect(set1, set2)

    elif compress is not None:
        if resmgr is not None:
            hostset = resmgr.compress_hosts(compress)
        else:
            hostset = compress.split(',')
            return scr_hostlist.compress_range(hostset)

    else:
        raise RuntimeError("scr_glob_hosts: Unknown set operation")

    # ok, got our resulting nodeset, now print stuff to the screen
    if nth is not None:
        # return the nth node of the nodelist
        n = int(nth)
        if n > len(hostset) or n < -len(hostset):
            raise RuntimeError(
                'scr_glob_hosts: ERROR: Host index (' + str(n) +
                ') is out of range for the specified host list.')
        if n > 0:  # an initial n=0 or n=1 both return the same thing
            n -= 1
        return hostset[n]

    # return the number of nodes (length) in the nodelist
    if count:
        return len(hostset)

    # return a csv string representation of the nodelist
    if resmgr is not None:
        hostset = resmgr.expand_hosts(hostset)
        return ','.join(hostset)
    else:
        return scr_hostlist.compress(hostset)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    # default=None, required=True, nargs='+'
    parser.add_argument('-c',
                        '--count',
                        action='store_true',
                        default=False,
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

    if ((args.hosts is None and args.minus is None
         and args.intersection is None and args.compress is None)
            or (args.minus is not None and ':' not in argso.minus) or
        (args.intersection is not None and ':' not in args.intersection)):
        parser.print_help()
        quit()

    result = scr_glob_hosts(count=args.count,
                            nth=args.nth,
                            hosts=args.hosts,
                            minus=args.minus,
                            intersection=args.intersection,
                            compress=args.compress)
    print(str(result))
