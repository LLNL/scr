import argparse

from scrjob import hostlist


def glob_compress(hoststr):
    """return compressed range form of hosts

    given a string like 'node1,node2,node3,node5'
    returns 'node[1-3,5]'
    """
    hosts = hostlist.expand_hosts(hoststr)
    return hostlist.compress_hosts(hosts)

def glob_minus(hoststr):
    """subtract nodes in second set from nodes in first

    given a string like 'node[1-6]:node[3,5]'
    returns 'node[1-2,4,6]'
    """
    pieces = hoststr.split(':')
    set1 = hostlist.expand_hosts(pieces[0])
    set2 = hostlist.expand_hosts(pieces[1])
    hosts = hostlist.diff_hosts(set1, set2)
    return hostlist.compress_hosts(hosts)

def glob_intersect(hoststr):
    """take the intersection of two nodesets

    given a string like 'node[1-6]:node[3,5,7]'
    returns 'node[3,5]'
    """
    pieces = hoststr.split(':')
    set1 = hostlist.expand_hosts(pieces[0])
    set2 = hostlist.expand_hosts(pieces[1])
    hosts = hostlist.intersect_hosts(set1, set2)
    return hostlist.compress_hosts(hosts)

def glob_nth(hoststr, n):
    """return the nth host of the hostlist, 1-based indexing

    given 'node[1-3,5-7]' and n=4
    return 'node5'
    """
    hosts = hostlist.expand_hosts(hoststr)
    if n > len(hosts) or n < -len(hosts):
        raise RuntimeError(
            f'Host index {n} is out of range for the specified host list.')
    if n > 0:  # an initial n=0 or n=1 both return the same thing
        n -= 1
    return hosts[n]

def glob_count(hoststr):
    """returns the number of hosts in a hoststr

    given 'node[1-3,5-7,10]'
    returns 7
    """
    hosts = hostlist.expand_hosts(hoststr)
    return len(hosts)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('hosts',
                       metavar='<hosts | hosts1:hosts2>',
                       type=str,
                       help='Use this hostlist.')

    group = parser.add_mutually_exclusive_group()
    group.add_argument('-m',
                       '--minus',
                       action='store_true',
                       help='Elements of s1 not in s2.')
    group.add_argument('-i',
                       '--intersection',
                       action='store_true',
                       help='Intersection of s1 and s2 hosts.')

    group2 = parser.add_mutually_exclusive_group()
    group2.add_argument('-e',
                       '--expand',
                       action='store_true',
                       help='Expand host list.')
    group2.add_argument('-C',
                       '--compress',
                       action='store_true',
                       help='Compress the csv hostlist.')
    group2.add_argument('-c',
                        '--count',
                        action='store_true',
                        help='Print the number of hosts.')
    group2.add_argument('-n',
                        '--nth',
                        metavar='<num>',
                        type=int,
                        default=None,
                        help='Output the Nth host (1=lo, -1=hi).')

    args = parser.parse_args()

    hoststr = args.hosts

    if args.minus:
        hoststr = glob_minus(hoststr)
    elif args.intersection:
        hoststr = glob_intersection(hoststr)

    if args.expand:
        hosts = hostlist.expand_hosts(hoststr)
        hoststr = hostlist.join_hosts(hosts)
        print(hoststr)
    elif args.compress:
        print(glob_compress(hoststr))
    elif args.nth:
        print(glob_nth(hoststr, args.nth))
    elif args.count:
        print(glob_count(hoststr))
    else:
        print(hoststr)
