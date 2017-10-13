package scr_hostlist;
use strict;

# This package processes slurm-style hostlist strings.
#
# expand($hostlist)
#   returns a list of individual hostnames given a hostlist string
# compress(@hostlist)
#   returns an ordered hostlist string given a list of hostnames
#
# Author:  Adam Moody (moody20@llnl.gov)


# Returns a list of hostnames, give a hostlist string
# expand("rhea[2-4,6]") returns ('rhea2','rhea3','rhea4','rhea6')

# modified by Christopher Holguin <christopher.a.holguin@intel.com>
#   notes: there are 2 new features 1) it now supports hostnames like rhea-1
#     and 2) rhea-1vm1


sub expand {
  # read in our hostlist, should be first parameter
  if (@_ != 1) {
    return undef;
  }
  my $nodeset = shift @_;
  # produce our list of nodes
  my @nodes = ();
  my $machine = undef;
  my @lowhighs = ();
  my @single_nodes = ();
  my $numberLength = 0; # for leading zeros, e.g atlas[0001-0003]
  if ($nodeset =~ /([\D\d]*)\[([\d,-]+)\](.*)/) {
    # hostlist with brackets, e.g., atlas[2-5,28,30]
    $machine = $1;
    my @ranges = split ",", $2;
    foreach my $range (@ranges) {
      my $low  = undef;
      my $high = undef;
      if ($range =~ /(\d+)-(\d+)/) {
        # low-to-high range
        $low  = $1;
        $high = $2;
        #if the lowest number starts with 0
        if($numberLength == 0 and index($low,"0") == 0){ 
            $numberLength = length($low);
        }
      } else {
        # single element range
        push @single_nodes, $range;
        #if the lowest number starts with 0
        if($numberLength == 0 and index($range,"0") == 0){ 
            $numberLength = length($range);
        }
        
        next;
      }

      push @lowhighs, $low, $high;
    }

    if($3 ne ""){
        my $temp_csv = $3;
        $temp_csv = substr($temp_csv, 1, length($temp_csv));
        my @extra_ranges = split ",", $temp_csv;
        push @nodes, @extra_ranges;
    }
  } else {
    # supports a simple list of host names, e.g., atlas2,atlas3,atlas4
    my @items = split ",", $nodeset;
    foreach my $item (@items) {
        # single host name, e.g., atlas2
        $item =~ /([\D]*)(\d+.*)/;
        $machine = $1;
        $numberLength = length($2);
        push @single_nodes, $2;
    }
  }


  while(@lowhighs) {
    my $low  = shift @lowhighs;
    my $high = shift @lowhighs;
    for(my $i = $low; $i <= $high; $i++) {
      my $nodenumber = sprintf("%0*d", $numberLength, $i);
      push @nodes, $machine . $nodenumber;
    }
  }
  while(@single_nodes){
      my $temp = shift @single_nodes;
      my $nodenumber = sprintf("%0*s", $numberLength, $temp);
      push @nodes, $machine . $temp;
  }
  
  return @nodes;
}

## Returns a hostlist string given a list of hostnames
## compress('rhea2','rhea3','rhea4','rhea6') returns "rhea[2-4,6]"
#sub compress {
#  if (@_ == 0) {
#    return "";
#  }
#
#  # pull the machine name from the first node name
#  my @numbers = ();
#  my @vmnumbers = ();
#  my ($machine) = ($_[0] =~ /([\D]*)(\d+.*)/);
#  foreach my $host (@_) {
#    # get the machine name and node number for this node
#    my ($name, $number) = ($host =~ /([\D]*)(\d+.*)/);
#
#    # check that all nodes belong to the same machine
#    if ($name ne $machine) {
#      return undef;
#    }
#    my ($temp_comp) = ($number =~ /([\d]+)/);
#    if ( $number eq $temp_comp ){
#        # record the number
#        push @numbers, $number;
#    }
#    else{
#        # we have a machine number with letters attached, so add to 
#        # separate list (we're not going to truly compress these)
#        push @vmnumbers, $machine . $number;
#    }
#
#  }
#
#  # order the nodes by number
#  my @sorted = sort {$a <=> $b} @numbers;
#
#  # TODO: toss out duplicates?
#
#  # build the ranges
#  my @ranges = ();
#  my $low  = $sorted[0];
#  my $last = $low;
#  for(my $i=1; $i < @sorted; $i++) {
#    my $high = $sorted[$i];
#    if($high == $last + 1) {
#      $last = $high;
#      next;
#    }
#    if($last > $low) {
#      push @ranges, $low . "-" . $last;
#    } else {
#      push @ranges, $low;
#    }
#    $low  = $high;
#    $last = $low;
#  }
#  if(@sorted > 0 ){
#      if($last > $low) {
#          push @ranges, $low . "-" . $last;
#      } else {
#          push @ranges, $low;
#      }
#  }
#  my $csv_vals = "";
#  if(@vmnumbers > 0){
#      $csv_vals = ",";
#      $csv_vals .= join(",", @vmnumbers);
#  }
#
#  if(@ranges == 0 && $csv_vals ne ""){
#      return  substr($csv_vals, 1, length($csv_vals));
#  }
#
#  # join the ranges with commas and return the compressed hostlist
#  return $machine . "[" . join(",", @ranges) . "]" . $csv_vals;
#}

# Returns a hostlist string given a list of hostnames
# compress('rhea2','rhea3','rhea4','rhea6') returns "rhea2,rhea3,rhea4,rhea6"
sub compress {
  if (@_ == 0) {
    return "";
  }

  # sort the node names and join them with commas
  my @nodes = sort {$a cmp $b} @_;
  return join(",", @nodes);
}

# Given references to two lists, subtract elements in list 2 from list 1 and return remainder
sub diff {
  # we should have two list references
  if (@_ != 2) {
    return undef;
  }
  my $set1 = $_[0];
  my $set2 = $_[1];

  my %nodes = ();

  # build list of nodes from set 1
  foreach my $node (@$set1) {
    $nodes{$node} = 1;
  }

  # remove nodes from set 2
  foreach my $node (@$set2) {
    delete $nodes{$node};
  }

  my @nodelist = (keys %nodes);
  if (@nodelist > 0) {
    my $list = scr_hostlist::compress(@nodelist);
    return scr_hostlist::expand($list);
  }
  return ();
}

# Given references to two lists, return list of intersection nodes
sub intersect {
  # we should have two list references
  if (@_ != 2) {
    return undef;
  }
  my $set1 = $_[0];
  my $set2 = $_[1];

  my %nodes = ();

  # build list of nodes from set 1
  my %tmp_nodes = ();
  foreach my $node (@$set1) {
    $tmp_nodes{$node} = 1;
  }

  # remove nodes from set 2
  foreach my $node (@$set2) {
    if (defined $tmp_nodes{$node}) {
      $nodes{$node} = 1;
    }
  }

  my @nodelist = (keys %nodes);
  if (@nodelist > 0) {
    my $list = scr_hostlist::compress(@nodelist);
    return scr_hostlist::expand($list);
  }
  return ();
}

1;
