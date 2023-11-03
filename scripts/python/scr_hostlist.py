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
                       metavar='HOSTS',
                       type=str,
                       nargs='+',
                       help='Provide one or more host lists.')

    parser.add_argument('-d',
                        '--delimiter',
                        metavar='<c>',
                        type=str,
                        default=',',
                        help='Delimeter character for --expand (default \',\').')
    parser.add_argument('-s',
                        '--size',
                        metavar='<N>',
                        type=int,
                        default=None,
                        help='Output at most N hosts (-N for last N hosts).')

    group = parser.add_mutually_exclusive_group()
    group.add_argument('-m',
                       '--minus',
                       action='store_true',
                       help='Subtract subsequent HOSTS from first HOSTS.')
    group.add_argument('-i',
                       '--intersection',
                       action='store_true',
                       help='Intersect subsequent HOSTS with first HOSTS.')

    group2 = parser.add_mutually_exclusive_group()
    group2.add_argument('-e',
                       '--expand',
                       action='store_true',
                       help='Expand host list instead of collapsing.')
    group2.add_argument('-c',
                        '--count',
                        action='store_true',
                        help='Print the number of hosts.')
    group2.add_argument('-n',
                        '--nth',
                        metavar='<N>',
                        type=int,
                        default=None,
                        help='Output the host at index N (-N to index from end).')

    args = parser.parse_args()

    # expand host string into a list of hosts
    hostvals = args.hosts
    if args.minus:
        # from first list, subtract all others
        hosts = hostlist.expand_hosts(hostvals[0])
        for h in hostvals[1:]:
            hosts2 = hostlist.expand_hosts(h)
            hosts = hostlist.diff_hosts(hosts, hosts2)
    elif args.intersection:
        # intersection of all lists
        hosts = hostlist.expand_hosts(hostvals[0])
        for h in hostvals[1:]:
            hosts2 = hostlist.expand_hosts(h)
            hosts = hostlist.intersect_hosts(hosts, hosts2)
    else:
        hosts = []
        for h in hostvals:
            hosts.extend(hostlist.expand_hosts(h))

    # cut list down to size N if requested
    if args.size is not None:
        size = args.size
        if size >= 0:
            hosts = hosts[:size]
        else:
            hosts = hosts[size:]

    if args.expand:
        # allow user to specify \n, and \t
        # TODO: there must be a less hacky way to do this
        c = args.delimiter
        if c == "\\n":
            c = '\n'
        elif c == "\\t":
            c = '\t'
        print(c.join(hosts))
    elif args.nth:
        n = args.nth
        if n > len(hosts) or n < -len(hosts):
            raise RuntimeError(
                f'Host index {n} is out of range for the specified host list.')
        if n > 0:  # an initial n=0 or n=1 both return the same thing
            n -= 1
        print(hosts[n])
    elif args.count:
        print(len(hosts))
    else:
        print(hostlist.compress_hosts(hosts))
