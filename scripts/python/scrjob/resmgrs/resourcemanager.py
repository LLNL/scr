class ResourceManager(object):
    """ResourceManager is the super class for the resource manager family."""

    def __init__(self, resmgr='UNKNOWN'):
        self.name = resmgr

    def prerun(self):
        """This method is called during prerun.py.

        Any necessary preamble work can be inserted into this method.
        This method does nothing by default and may be overridden as
        needed.
        """
        pass

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
