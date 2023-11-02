class ResourceManager(object):
    """ResourceManager is the super class for the resource manager family.

    Provided Default Methods
    ------------------------
    use_watchdog()
      allows setting of self.watchdog and getting its value

    Other Methods
    -------------
    Other methods should be implemented in derived classes as appropriate.

    Attributes
    ----------
    resmgr               - String representation of the resource manager
    watchdog             - A boolean indicating whether to use the watchdog method
    """

    def __init__(self, resmgr='unknown'):
        self.resmgr = resmgr
        self.watchdog = False

    def use_watchdog(self, watchdog=None):
        """Set or get the watchdog attribute.

        When not using the Watchdog, a jobstep is launched and waited on.

        When using the Watchdog, a jobstep is launched and waited on until a timeout.
        At each timeout, if the process is still running, a check is conducted for
        the existence of output files. If no new output files exist, it will be assumed
        that the launched process is hanging and it will be terminated.

        Given a boolean parameter, this method will set the watchdog attribute.
        Called without a parameter, the value will be returned.

        Returns
        -------
        bool
            indicates whether watchdog is active
        """
        if watchdog is None:
            return self.watchdog
        self.watchdog = watchdog

    def job_id(self):
        """Return current job allocation id.

        This value is used in logging and is used in building paths for output files.

        Returns
        -------
        str
            job allocation id
            or None if unknown or error
        """
        return None

    def job_nodes(self):
        """Return compute nodes in the allocation.

        Each node should be specified once and in order.

        Returns
        -------
        list(str)
            list of allocation compute nodes
            empty if error
        """
        return []

    def down_nodes(self):
        """Return allocation compute nodes the resource manager identifies as
        down.

        Some resource managers can report nodes it has determined to be down.
        The returned list should be a subset of the allocation nodes.

        Returns
        -------
        dict
            { 'node' : 'reason', ... }
            dict of down compute nodes, keyed by the node, reason is a description of
            why a node is down: 'Reported down by resource manager' or 'Excluded by resource manager'
        """
        return {}

    def end_time(self):
        """Return expected allocation end time.

        The end time must be expressed as seconds since
        the Unix epoch.

        Returns
        -------
        int
            end time as secs since Unix epoch, if end time is determined
            0, if end time is unknown or error
            -1, if there is no end time
        """
        return 0
