# Determine whether the job script should exit or launch another run.
# Checks that:
#   - a halt condition has not been set
#   - there is sufficient time remaining in the allocation
#   - there are sufficient healthy nodes in the allocation
# Returns True if job should exit, False otherwise
# Raises RuntimeError on failure

import os

from scrjob.cli import SCRRetriesHalt


# determine how many nodes are needed
def nodes_needed(jobenv, nodelist):
    # if SCR_MIN_NODES is set, use that
    num_needed = os.environ.get('SCR_MIN_NODES')
    if num_needed is None or int(num_needed) <= 0:
        # otherwise, use value in nodes file if one exists,
        # records number of nodes used in previous run
        num_needed = jobenv.runnode_count()
        if num_needed <= 0:
            # otherwise, assume we need all nodes in the allocation
            num_needed = len(nodelist)
    return int(num_needed)


# return number of nodes left in allocation after excluding down nodes
def nodes_remaining(nodelist, down_nodes):
    temp = [n for n in nodelist if n not in down_nodes]
    return len(temp)


def should_exit(jobenv, down_nodes=[], min_nodes=None, verbose=False):
    prefix = jobenv.dir_prefix()

    # get the nodeset of this job
    nodelist = jobenv.node_list()
    if not nodelist:
        nodelist = jobenv.resmgr.job_nodes()
    if not nodelist:
        raise RuntimeError(f'Could not identify nodeset for job')

    # TODO: read SCR_HALT_SECONDS and current time
    # This should be handled indirectly by the library,
    # since it will record a halt condition.
    # look up allocation end time, record in SCR_END_TIME
    #endtime = jobenv.resmgr.end_time()

    # is there a halt condition instructing us to stop?
    halt = SCRRetriesHalt(prefix)
    halt_cond = halt.check()
    if halt_cond:
        if verbose:
            print(f'Halt condition detected: {halt_cond}')
        return True

    # bail out if we don't have enough nodes to continue
    # determine how many nodes are needed
    num_needed = min_nodes
    if num_needed is None:
        num_needed = nodes_needed(jobenv, nodelist)
        if num_needed <= 0:
            raise RuntimeError('Unable to determine number of nodes needed')

    # determine number of nodes remaining in allocation
    num_left = nodes_remaining(nodelist, down_nodes)

    # check that we have enough nodes after excluding down nodes
    if num_left < num_needed:
        if verbose:
            print(f'nodes_remaining={num_left} < nodes_needed={num_needed}')
        return True

    # everything checks out, the job can keep going
    return False
