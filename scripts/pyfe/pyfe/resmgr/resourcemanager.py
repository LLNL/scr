#! /usr/bin/env python3
"""
resmgr/resourcemanager.py

ResourceManager is the super class for the resource manager family.
Node set methods (compress, expand, diff, intersect) are provided by this class.
If the constant, USE_CLUSTERSHELL, is not '0' and the module ClusterShell is available
then ClusterShell.NodeSet will be used for these operations.

Attributes
----------
clustershell_nodeset - Either False or a pointer to the module ClusterShell.NodeSet
prefix               - String returned from scr_prefix()
resmgr               - String representation of the resource manager
use_watchdog         - A boolean indicating whether to use the watchdog method
nodes                - Comma separated string of nodes in the current allocation.

Methods
-------
init()
    This method initializes class attributes.
get_jobstep_id(user, pid) ###
    This method can be removed if the watchdog works ok. ***
    Takes the user (string) and the pid.
    This returned an ID to be used in the kill jobstep command for the watchdog.
getjobid()
    This method returns the Job ID as a string.
    The Job ID is how the resource manager refers to the current allocation.
    None is returned when the Job ID cannot be determined or is not applicable.
get_job_nodes()
    This method returns a comma separated string of nodes in the current allocation.
    None is returned when this cannot be determined.
      (If None is returned here the nodelist must be obtainable through scr_env.get_scr_nodelist)
get_downnodes()
    This method returns a list of nodes the resource manager thinks to be down.
    None may be returned if there are no down nodes or this cannot be determined.
scr_kill_jobstep(jobid) ###
    This method can be removed if the watchdog works ok. ***
    This method has the resource manager kill the jobstep id returned by the above noted method.
get_scr_end_time()
    This method returns an integer, the posix time for the end of the current allocation.
    Return 0 for error, or if the time cannot be determined.
    Return -1 to indicate that there is no end time / limit.
list_down_nodes_with_reason(nodes=[],scr_env=None,free=False,cntldir_string=None,cachedir_string=None) ###
    This method receives a list of nodes to test to determine if they are still up.
    This method returns a dictionary of nodes, keyed by nodes with reasons as values.
      e.g., return { 'node1' : 'Failed ping' }
    *** The parameters of this method should be simplified ***

usewatchdog(use_scr_watchdog)
    This method implementation is provided by the base class.
    This method is called with an optional Boolean parameter.
    If the parameter is not given, the current value of the attribute is returned.
    If the parameter is given, the attribute is assigned to the value of the parameter.
compress_hosts(hostnames)
    This method implementation is provided by the base class.
    This method receives a list (or string) or hostnames.
    The host names will be converted to a compressed format, e.g., 'node[1-7]'
    This method returns a comma separated list of hosts as a string.
expand_hosts(hostnames='')
    This method implementation is provided by the base class.
    This method receives a string (or list) of hostnames.
    The hostnames will be returned as a list in expanded format, e.g., [node1, node2, node3]
diff_hosts(set1=[],set2=[])
    This method implementation is provided by the base class.
    This method receives 2 lists (or strings) of hostnames.
    A list will be returned of the set difference of these 2 lists.
intersect_hosts(set1=[], set2=[])
    This method implementation is provided by the base class.
    This method receives 2 lists (or strings) of hostnames.
    A list will be returned of the set intersection of these 2 lists.
get_scavenge_nodelists(upnodes, downnodes)
    This method implementation is provided by the base class.
    This method receives 2 nodelist strings (or lists).
    This returns 2 nodelist strings which will be provided to the job launcher during scavenge operations.
    The returned strings are a comma separated list of upnodes and a space separated list of downnodes.
"""

import os
from pyfe import scr_const, scr_hostlist
from pyfe.scr_common import scr_prefix


class ResourceManager(object):
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
    self.nodes = self.get_job_nodes()

  def usewatchdog(self, use_scr_watchdog=None):
    """Set or get the use_scr_watchdog attribute

    Given a boolean parameter will set the use_scr_watchdog attribute
    Called without a parameter value will return the use_scr_watchdog attribute

    Returns
    -------
    bool
        use_scr_watchdog
        or None if parameter given and attribute was set
    """
    if use_scr_watchdog is None:
      return self.use_watchdog
    self.use_watchdog = use_scr_watchdog

  def getjobid(self):
    """Return current job allocation id.

    Returns
    -------
    str
        job allocation id
        or None if unknown or error
    """
    return None

  def get_job_nodes(self):
    """Return compute nodes in allocation.

    Each node should be specified once and in order
    if node order matters, e.g., for job launchers.

    Returns
    -------
    str
        list of allocation compute nodes in string format
        or None if unknown or error
    """
    return None

  def get_downnodes(self):
    """Return allocation compute nodes the resource manager identifies as down.

    Some resource managers can report nodes it has determined to be down.
    The returned list should be a subset of the allocation nodes.

    Returns
    -------
    list
        list of down compute nodes
        or None if there are no down nodes
    """
    return None

  def get_scr_end_time(self):
    """Return expected allocation end time.

    The end time must be expressed as seconds since
    the Unix epoch.

    Returns
    -------
    int
        end time as secs since Unix epoch
        or None if there is no end time
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
      nodeset = [node for node in nodeset]
      return nodeset
    return scr_hostlist.expand(hostnames)

  def diff_hosts(self, set1=[], set2=[]):
    """Return the set difference from 2 host lists

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
      set1 = [node for node in set1]
      return set1
    return scr_hostlist.diff(set1=set1, set2=set2)

  def intersect_hosts(self, set1=[], set2=[]):
    """Return the set intersection of 2 host lists

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
      set1 = [node for node in set1]
      return set1
    return scr_hostlist.intersect(set1, set2)

  # return a hash to define all unavailable (down or excluded) nodes and reason
  def list_down_nodes_with_reason(self,
                                  nodes=[],
                                  scr_env=None,
                                  free=False,
                                  cntldir_string=None,
                                  cachedir_string=None):
    """Return down nodes with the reason they are down

    Input parameter, nodes, is a list or a comma separated string.
    ### Other input parameters? scr_env?
    ### Could pass in a set of down nodes reported from scr_env to (add to)/(ignore).

    Returns
    -------
    dict
        dictionary of reported down nodes, keyed by node with reasons as values
    """
    return {}

  # each scavenge operation needs upnodes and downnodes_spaced
  def get_scavenge_nodelists(self, upnodes='', downnodes=''):
    """Return formatted upnodes and downnodes for joblaunchers' scavenge operation

    Input parameters upnodes and downnodes are comma separated strings or lists.

    Returns
    -------
    str
        comma separated list of up nodes
    str
        space separated list of down nodes
    """
    if type(upnodes) is list:
      upnodes = ','.join(upnodes)
    if type(downnodes) is list:
      downnodes = ','.join(downnodes)
    # get nodesets
    jobnodes = self.get_job_nodes()
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
    upnodes = self.compress_hosts(upnodes)
    downnodes_spaced = ' '.join(downnodes)
    return upnodes, downnodes_spaced


if __name__ == '__main__':
  resmgr = ResourceManager()
  print(type(resmgr))
