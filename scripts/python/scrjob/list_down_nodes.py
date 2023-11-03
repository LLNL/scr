def list_down_nodes(jobenv,
                    nodes=None,
                    nodes_down=[],
                    free=False,
                    reason=False,
                    log=None,
                    secs=None):
    """Identifies set of nodes determined to be down.

    Requires a valid jobenv object.

    The nodes to be checked should be given in nodes. If nodes is not
    given, the jobenv will be queried to get the list of nodes in the
    allocation.

    Any nodes specified in nodes_down will be considered to be down
    without checking. This is useful to keep a node down that was
    reported as down earlier, but have since come back online.

    The free parameter is not currently in use. Its intent is to direct
    the storage capacity test to check free space vs total capacity.

    If given a log object, logs nodes as NODE_FAIL. One can then
    additionally specify the number of seconds since the run started in
    secs.

    If reason=False, returns a list of node names. If reason=True,
    returns a dictionary, where the node name is the key and the reason
    the node is determined to be down is the value.
    """

    # check that we have a list of nodes before going any further
    if not nodes:
        nodes = jobenv.node_list()
    if not nodes:
        nodes = jobenv.resmgr.job_nodes()
    if not nodes:
        raise RuntimeError('Failed to identify nodes in job allocation.')

    # get a dictionary of all unavailable (down or excluded) nodes and reason
    # keys are nodes and the values are the reasons
    unavailable = jobenv.nodetests.execute(nodes, jobenv)

    # log each newly failed node, along with the reason
    if log:
        for node, reason in unavailable.items():
            note = node + ": " + reason
            log.event('NODE_FAIL', note=note, secs=secs)

    if not reason:
        return sorted(list(unavailable.keys()))

    return unavailable
