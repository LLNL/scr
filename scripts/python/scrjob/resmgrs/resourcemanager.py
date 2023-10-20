import os
from scrjob import scr_const, scr_hostlist
from scrjob.scr_common import scr_prefix
from scrjob.resmgrs import Nodetests


class ResourceManager(object):
    """ResourceManager is the super class for the resource manager family.

    Provided Default Methods
    ------------------------
    usewatchdog()
      allows setting of self.use_watchdog and getting its value

    If the constant, USE_CLUSTERSHELL, is not '0' and the module ClusterShell is available
    then ClusterShell.NodeSet will be used for these operations.

    Node set methods are provided by this class:

    compress_hosts()
      returns a string representing a nodelist in compressed format

    expand_hosts()
      returns a list from a nodelist where each element is a unique host

    diff_hosts()
      returns the set difference of 2 node lists

    intersect_hosts()
      returns the intersection of 2 node lists

    list_down_nodes_with_reason()
      returns a dictionary of nodes reported down according to tests in self.nodetests

    scavenge_nodelists()
      returns upnodes and downnodes formatted for scr_scavenge

    Other Methods
    -------------
    Other methods should be implemented in derived classes as appropriate.

    Attributes
    ----------
    clustershell_nodeset - Either False or a pointer to the module ClusterShell.NodeSet
    prefix               - String returned from scr_prefix()
    resmgr               - String representation of the resource manager
    use_watchdog         - A boolean indicating whether to use the watchdog method
    nodetests            - An instance of the Nodetests class
    """

    def __init__(self, resmgr='unknown'):
        self.clustershell_nodeset = False
        if scr_const.USE_CLUSTERSHELL != '0':
            try:
                import ClusterShell.NodeSet as MyCSNodeSet
                self.clustershell_nodeset = MyCSNodeSet
            except:
                self.clustershell_nodeset = False
        self.prefix = scr_prefix()
        self.resmgr = resmgr
        self.use_watchdog = False
        self.nodetests = Nodetests()

    def prerun_tests(self):
        """This method returns a list of tests to perform during scr_prerun.

        Test methods must be defined in the SCR_Test_Runtime class in scr_test_runtime.py.
        Tests ensure the environment will function with SCR and may vary between environments.
        See scr_test_runtime.py for more information on tests available and to add additional tests.

        Returns
        -------
        list
            A list of strings, where each string is a static method in the SCR_Test_Runtime class
        """
        # The check_clustershell method only returns failure when ClusterShell is enabled yet
        # we are unable to import the ClusterShell module, this is a safe test for all managers
        return ['check_clustershell']

    def usewatchdog(self, use_scr_watchdog=None):
        """Set or get the use_scr_watchdog attribute.

        When not using the SCR_Watchdog, a jobstep is launched and waited on.

        When using the SCR_Watchdog, a jobstep is launched and waited on until a timeout.
        At each timeout, if the process is still running, a check is conducted for
        the existence of output files. If no new output files exist, it will be assumed
        that the launched process is hanging and it will be terminated.

        Given a boolean parameter, this method will set the use_scr_watchdog attribute.
        Called without a parameter, the value will be returned.

        Returns
        -------
        bool
            use_scr_watchdog
            or None if parameter given and attribute was set
        """
        if use_scr_watchdog is None:
            return self.use_watchdog
        self.use_watchdog = use_scr_watchdog

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
        str
            list of allocation compute nodes in string format
            or None if unknown or error
        """
        return None

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
            If end time is determined:       end time as secs since Unix epoch
            If end time is unknown or error: 0
            If there is no end time:         -1
        """
        return 0

    def compress_hosts(self, hostnames=[]):
        """Return hostlist string, where the hostlist is in a compressed form.

        Input parameter, hostnames, is a list or a comma separated string.

        Returns
        -------
        str
            comma separated hostlist in compressed form, e.g., 'node[1-4],node7'
        """
        if type(hostnames) is str:
            hostnames = hostnames.split(',')
        if hostnames is None or len(hostnames) == 0:
            return ''
        if self.clustershell_nodeset != False:
            nodeset = self.clustershell_nodeset.NodeSet.fromlist(hostnames)
            # the type is a ClusterShell NodeSet, convert to a string
            return str(nodeset)
        return scr_hostlist.compress_range(hostnames)

    def expand_hosts(self, hostnames=''):
        """Return list of hosts, where each element is a single host.

        Input parameter, hostnames, is a comma separated string or a list.

        Returns
        -------
        list
            list of expanded hosts, e.g., ['node1','node2','node3']
        """
        if type(hostnames) is list:
            hostnames = ','.join(hostnames)
        if hostnames is None or hostnames == '':
            return []
        if self.clustershell_nodeset != False:
            nodeset = self.clustershell_nodeset.NodeSet(hostnames)
            # the type is a ClusterShell NodeSet, convert to a list
            nodeset = [node for node in nodeset]
            return nodeset
        return scr_hostlist.expand(hostnames)

    def diff_hosts(self, set1=[], set2=[]):
        """Return the set difference from 2 host lists.

        Input parameters, set1 and set2, are lists or comma separated strings.

        Returns
        -------
        list
            elements of set1 that do not appear in set2
        """
        if type(set1) is str:
            set1 = set1.split(',')
        if type(set2) is str:
            set2 = set2.split(',')
        if set1 is None or set1 == []:
            return set2 if set2 is not None else []
        if set2 is None or set2 == []:
            return set1
        if self.clustershell_nodeset != False:
            set1 = self.clustershell_nodeset.NodeSet.fromlist(set1)
            set2 = self.clustershell_nodeset.NodeSet.fromlist(set2)
            # strict=False is default
            # if strict true then raises error if something in set2 not in set1
            set1.difference_update(set2, strict=False)
            # ( this should also work like set1 -= set2 )
            # the type is a ClusterShell NodeSet, convert to a list
            set1 = [node for node in set1]
            return set1
        return scr_hostlist.diff(set1=set1, set2=set2)

    def intersect_hosts(self, set1=[], set2=[]):
        """Return the set intersection of 2 host lists.

        Input parameters, set1 and set2, are lists or comma separated strings.

        Returns
        -------
        list
            elements of set1 that also appear in set2
        """
        if type(set1) is str:
            set1 = set1.split(',')
        if type(set2) is str:
            set2 = set2.split(',')
        if set1 is None or set1 == []:
            return []
        if set2 is None or set2 == []:
            return []
        if self.clustershell_nodeset != False:
            set1 = self.clustershell_nodeset.NodeSet.fromlist(set1)
            set2 = self.clustershell_nodeset.NodeSet.fromlist(set2)
            set1.intersection_update(set2)
            # the type is a ClusterShell NodeSet, convert to a list
            set1 = [node for node in set1]
            return set1
        return scr_hostlist.intersect(set1, set2)

    # return a hash to define all unavailable (down or excluded) nodes and reason
    def list_down_nodes_with_reason(self, nodes=[], scr_env=None):
        """Return down nodes with the reason they are down.

        Parameters
        ----------
        nodes     a list or a comma separated string
        scr_env   the SCR_Environment object

        The Nodetests object from resmgr/nodetests.py contains all tests.
        The tests which will be performed should be set either:
          When self.nodetests is instantiated (in init of Nodetests):
            by constant list in scr_const.py
            by file input, where the filename is specified in scr_const.py
          Or manually by adding test names to the self.nodetests.nodetests list
          in your resource manager's init after super().__init__

        Returns
        -------
        dict
            dictionary of reported down nodes, keyed by node with reasons as values
        """
        unavailable = self.nodetests(nodes=nodes, scr_env=scr_env)
        return unavailable

    # each scavenge operation needs upnodes and downnodes_spaced
    def scavenge_nodelists(self, upnodes='', downnodes=''):
        """Return formatted upnodes and downnodes for joblaunchers' scavenge
        operation.

        Input parameters upnodes and downnodes are comma separated strings or lists.

        Returns
        -------
        upnodes     string, a comma separated list of up nodes
        downnodes   string, a space separated list of down nodes
        """
        if type(upnodes) is list:
            upnodes = ','.join(upnodes)
        if type(downnodes) is list:
            downnodes = ','.join(downnodes)
        # get nodesets
        jobnodes = self.job_nodes()
        if jobnodes is None:
            ### error handling
            print('scr_scavenge: ERROR: Could not determine nodeset.')
            return '', ''
        jobnodes = self.expand_hosts(jobnodes)
        if downnodes != '':
            downnodes = self.expand_hosts(downnodes)
            upnodes = self.diff_hosts(jobnodes, downnodes)
        elif upnodes != '':
            upnodes = self.expand_hosts(upnodes)
            downnodes = self.diff_hosts(jobnodes, upnodes)
        else:
            upnodes = jobnodes
        ##############################
        # format up and down node sets for scavenge command
        #################
        upnodes = ','.join(self.expand_hosts(upnodes))
        downnodes_spaced = ' '.join(self.expand_hosts(downnodes))
        return upnodes, downnodes_spaced
