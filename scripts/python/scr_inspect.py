import os
import sys
import argparse
import re

from scrjob import config, hostlist
from scrjob.jobenv import JobEnv


def scr_inspect(jobnodes=None, up=None, down=None, cntldir=None, jobenv=None):
    """This method runs scr_inspect_cache on each node using
    Joblauncher.parallel_exec.

    Returns
    -------
    string   A space separated list of cached datasets which may be able to flush and rebuild

    On error this method returns the integer 1
    """
    libexecdir = config.X_LIBEXECDIR
    pdsh = config.PDSH_EXE

    if jobenv is None:
        jobenv = JobEnv()

    # tag output files with jobid
    jobid = jobenv.job_id()
    if jobid is None:
        raise RuntimeError('Could not determine jobid.')

    # read node set of job
    jobset = jobenv.node_list()
    if jobset is None:
        jobset = jobenv.resmgr.job_nodes()
        if jobset is None:
            raise RuntimeError('Could not determine nodeset.')

    # can't get directories
    if cntldir is None:
        raise RuntimeError('Control directory must be specified.')

    # get nodesets
    if jobnodes is None:
        raise RuntimeError('Job nodes must be specified.')

    jobnodes = hostlist.expand_hosts(jobnodes)

    upnodes = []
    downnodes = []
    if down is not None:
        downnodes = hostlist.expand_hosts(down)
        upnodes = hostlist.diff_hosts(jobnodes, downnodes)
    elif up is not None:
        upnodes = hostlist.expand_hosts(up)
        downnodes = hostlist.diff_hosts(jobnodes, upnodes)
    else:
        upnodes = jobnodes

    # make the list a comma separated string
    upnodes = ','.join(upnodes)

    # build the output filenames
    pwd = os.getcwd()
    scr_dir = os.path.join(pwd, '.scr')
    os.makedirs(scr_dir, exist_ok=True)
    outfile = os.path.join(scr_dir, 'scr_inspect.pdsh.o.' + jobid)
    errfile = os.path.join(scr_dir, 'scr_inspect.pdsh.e.' + jobid)

    # run scr_inspect_cache via pdsh / clustershell
    argv = [
        os.path.join(libexecdir, 'scr_inspect_cache'),
        os.path.join(cntldir, 'filemap.scrinfo')
    ]
    out = jobenv.launcher.parallel_exec(argv=argv, runnodes=upnodes)[0]
    try:
        with open(outfile, 'w') as f:
            f.write(out[0])
    except Exception as e:
        print('scr_inspect: ERROR: Error writing scr_inspect_cache stdout')
        print(e)

    try:
        with open(errfile, 'w') as f:
            f.write(out[1])
    except Exception as e:
        print('scr_inspect: ERROR: Error writing scr_inspect_cache stderr')
        print(e)

    # scan output file for list of partners and failed copies
    groups = {}
    types = {}

    # open the file, exit with error if we can't
    readout = False
    try:
        with open(outfile, 'r') as f:
            readout = True
            for line in f.readlines():
                line = line.rstrip()

                search = re.search(
                    r'DSET=(\d+) RANK=(\d+) TYPE=(\w+) GROUPS=(\d+) GROUP_ID=(\d+) GROUP_SIZE=(\d+) GROUP_RANK=(\d+)',
                    line)
                if search is not None:
                    dset = int(search.group(1))
                    rank = int(search.group(2))
                    atype = search.group(3)
                    ngroups = int(search.group(4))
                    group_id = int(search.group(5))
                    group_size = int(search.group(6))
                    group_rank = int(search.group(7))

                    if dset not in groups:
                        groups[dset] = {}
                        groups[dset]['ids'] = {}

                    if group_id not in groups[dset]['ids']:
                        groups[dset]['ids'][group_id] = {}
                        groups[dset]['ids'][group_id]['ranks'] = {}

                    groups[dset]['ids'][group_id]['ranks'][group_rank] = 1
                    groups[dset]['ids'][group_id]['size'] = group_size
                    groups[dset]['groups'] = ngroups
                    types[dset] = atype

    except Exception as e:
        print(e)
        raise RuntimeError('Reading and processing output file \"' + output +
                           '\"')

    # starting with the most recent dataset, check whether we have (or may be able to recover) all files
    possible_dsets = []
    dsets = sorted(list(groups.keys()))
    for dset in dsets:
        # get the expected number of groups and the dataset type
        expected_groups = groups[dset]['groups']
        atype = types[dset]

        # count the number of groups we find, and the number we can't recover
        num_groups = 0
        missing_groups = 0
        sortedids = list(groups[dset]['ids'].keys())
        sortedids.sort()
        for group in sortedids:
            # add this group to our running total, and get its size
            num_groups += 1
            group_size = groups[dset]['ids'][group]['size']

            # count the number of ranks we're missing from this dataset
            missing_ranks = []
            for i in range(group_size):
                if i not in groups[dset]['ids'][group]['ranks']:
                    missing_ranks.append(i)

            # determine whether we are missing too many ranks from this group based on the dataset type
            missing_too_many = False
            if (type == 'LOCAL'
                    or type == 'PARTNER') and len(missing_ranks) > 0:
                missing_too_many = True
            elif type == 'XOR' and len(missing_ranks) > 1:
                missing_too_many = True

            # if we're missing too many ranks from this group, add it to the total
            if missing_too_many == True:
                missing_groups += 1

        # if we have a chance to recover files from all groups of this dataset, add it to the list
        if num_groups == expected_groups and missing_groups == 0:
            possible_dsets.append(dset)

    # return the list the datasets we have a shot of recovering
    return possible_dsets


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        epilog=
        'The jobid and job node set must be able to be obtained from the environment.'
    )
    parser.add_argument('-j',
                        '--jobset',
                        default=None,
                        metavar='<nodeset>',
                        type=str,
                        required=True,
                        help='Job nodes.')
    parser.add_argument('-u',
                        '--up',
                        default=None,
                        metavar='<nodeset>',
                        type=str,
                        help='Up nodes.')
    parser.add_argument('-d',
                        '--down',
                        default=None,
                        metavar='<nodeset>',
                        type=str,
                        help='Down nodes.')
    parser.add_argument('-f',
                        '--from',
                        default=None,
                        metavar='<ctrl dir>',
                        type=str,
                        required=True,
                        help='Control directory.')

    args = parser.parse_args()

    try:
        dsets = scr_inspect(jobnodes=args.jobset,
                            up=args.up,
                            down=args.down,
                            cntldir=args['from'])
        print(' '.join(dsets))
    except Exception as e:
        print('scr_inspect: ERROR: ' + str(e))
        sys.exit(1)
