# this is a standalone script which queries the class JobEnv
# JobEnv contains general values from the environment

import os
import sys
import argparse

from scrjob import hostlist
from scrjob.jobenv import JobEnv

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-u',
                        '--user',
                        action='store_true',
                        help='List the username of current job.')
    parser.add_argument('-j',
                        '--jobid',
                        action='store_true',
                        help='List the job id of the current job.')
    parser.add_argument('-e',
                        '--endtime',
                        action='store_true',
                        help='List the end time of the current job.')
    parser.add_argument('-n',
                        '--nodes',
                        action='store_true',
                        help='List the nodeset the current job is using.')
    parser.add_argument(
        '-d',
        '--down',
        action='store_true',
        help=
        'List any nodes of the job\'s nodeset that the resource manager knows to be down.'
    )
    parser.add_argument('-p',
                        '--prefix',
                        metavar='<dir>',
                        type=str,
                        default=None,
                        help='Specify the prefix directory.')
    parser.add_argument('-r',
                        '--runnodes',
                        action='store_true',
                        help='List the number of nodes used in the last run.')

    args = parser.parse_args()

    jobenv = JobEnv(prefix=args.prefix)

    if args.user:
        print(str(jobenv.user()), end='')

    if args.jobid:
        print(str(jobenv.resmgr.job_id()), end='')

    if args.endtime:
        print(str(jobenv.resmgr.end_time()), end='')

    if args.nodes:
        nodelist = jobenv.node_list()
        if not nodelist:
            nodelist = jobenv.resmgr.job_nodes()
        nodestr = hostlist.join_hosts(nodelist)
        print(nodestr, end='')

    if args.down:
        nodelist = jobenv.resmgr.down_nodes()
        nodestr = hostlist.join_hosts(sorted(nodelist.keys()))
        print(nodestr, end='')

    if args.runnodes:
        print(str(jobenv.runnode_count()), end='')
