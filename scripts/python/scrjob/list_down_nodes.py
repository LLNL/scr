def list_down_nodes(jobenv,
                    nodes=None,
                    nodes_down=[],
                    free=False,
                    reason=False,
                    log=None,
                    secs=None):
    """Identifies set of nodes determined to be down.

    Requires a valid jobenv object.

    The list of nodes to check can be given in nodes. If not given, the
    jobenv is queried to get the list of nodes in the allocation.

    Any nodes specified in nodes_down are considered to be down without
    checking. This is useful to keep a node down that was earlier
    reported as down but has since come back online.

    The free parameter is not currently used. Its intent is to configure
    the storage capacity test to check free drive space vs total
    capacity.

    If given a log object, appends NODE_FAIL messages to the log. One
    can specify the number of seconds since the run started in secs.

    If reason=False, list_down_nodes returns a list of node names. If
    reason=True, list_down_nodes returns a dictionary, where the key is
    the node name and the value is a string giving the reason that the
    node is determined to be down.
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
    unavailable = jobenv.nodetests.execute(nodes, jobenv, down=nodes_down)

    # log each newly failed node, along with the reason
    if log:
        for node, reason in unavailable.items():
            note = node + ": " + reason
            log.event('NODE_FAIL', note=note, secs=secs)

    if not reason:
        return sorted(list(unavailable.keys()))

    return unavailable
