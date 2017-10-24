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
# modified by Christopher Holguin <christopher.a.holguin@intel.com>

# Returns a list of hostnames given a hostlist string
# expand("rhea[2-4,6]") returns ('rhea2','rhea3','rhea4','rhea6')
# hostrange can contain an optional suffix after brackets:
#   rhea[2-4,6].llnl.gov
# multiple ranges can be listed as csv:
#   machine[1-3,5],machine[7-8],machine10
# left fills with 0 if a host starts with 0:
#   machine[08-10] --> machine08,machine09,machine10
sub expand {
  # read in our hostlist, should be first parameter
  if (@_ != 1) {
    return undef;
  }
  my $nodeset = shift @_;

  my @nodes = ();

  # split entries on commas
  # machine[1-3,5],machine[7-8],machine10
  my @chunks = split ",", $nodeset;
  while (@chunks > 0) {
    # look for opening bracket
    my $chunk = shift @chunks;
    if ($chunk =~ /^(.*)\[(.*)$/) {
      # got a starting bracket, scan until we find the closing bracket
      my $prefix  = $1;
      my $content = $2;
      my $suffix  = "";

      # build a list of ranges until we find the closing bracket
      my @ranges = ();
      if ($content =~ /^(.*)\](.*)$/) {
        # found the closing bracket (and optional suffix) in the same chunk
        push @ranges, $1;
        $suffix = $2;
      } else {
        # no bracket, so we either got a single number or a range here
        push @ranges, $content;

        # pluck off entries until we find the closing bracket
        while ($chunks[0] !~ /^(.*)\](.*)$/) {
          $chunk = shift @chunks;
          push @ranges, $chunk;
        }

        # if well formed, this item must now have the bracket
        $chunk = shift @chunks;
        if ($chunk =~ /^(.*)\](.*)$/) {
          # found the closing bracket (and optional suffix)
          push @ranges, $1;
          $suffix = $2;
        }
      }

      # expand ranges to pairs of low/high values
      my @lowhighs = ();
      my $numberLength = 0; # for leading zeros, e.g atlas[0001-0003]
      foreach my $range (@ranges) {
        my $low  = undef;
        my $high = undef;
        if ($range =~ /(\d+)-(\d+)/) {
          # low-to-high range
          $low  = $1;
          $high = $2;
        } else {
          # single element range
          $low  = $range;
          $high = $range;
        }
        #if the lowest number starts with 0
        if($numberLength == 0 and index($low,"0") == 0){ 
           $numberLength = length($low);
        }
        push @lowhighs, $low, $high;
      }

      # produce our list of node names
      while(@lowhighs) {
        my $low  = shift @lowhighs;
        my $high = shift @lowhighs;
        for(my $i = $low; $i <= $high; $i++) {
          # tack on leading 0's if input had them
          my $nodenumber = sprintf("%0*d", $numberLength, $i);
          my $nodename = $prefix . $nodenumber . $suffix;
          push @nodes, $nodename;
        }
      }
    } else {
      # no brackets, just a single node name, copy it verbatim
      push @nodes, $chunk;
    }
  }

  return @nodes;
}

# Returns a hostlist string given a list of hostnames
# compress('rhea2','rhea3','rhea4','rhea6') returns "rhea[2-4,6]"
sub compress_range {
  if (@_ == 0) {
    return "";
  }

  # pull the machine name from the first node name
  my @numbers = ();
  my @vmnumbers = ();
  my ($machine) = ($_[0] =~ /([\D]*)(\d+.*)/);
  foreach my $host (@_) {
    # get the machine name and node number for this node
    my ($name, $number) = ($host =~ /([\D]*)(\d+.*)/);

    # check that all nodes belong to the same machine
    if ($name ne $machine) {
      return undef;
    }
    my ($temp_comp) = ($number =~ /([\d]+)/);
    if ( $number eq $temp_comp ){
        # record the number
        push @numbers, $number;
    }
    else{
        # we have a machine number with letters attached, so add to 
        # separate list (we're not going to truly compress these)
        push @vmnumbers, $machine . $number;
    }

  }

  # order the nodes by number
  my @sorted = sort {$a <=> $b} @numbers;

  # TODO: toss out duplicates?

  # build the ranges
  my @ranges = ();
  my $low  = $sorted[0];
  my $last = $low;
  for(my $i=1; $i < @sorted; $i++) {
    my $high = $sorted[$i];
    if($high == $last + 1) {
      $last = $high;
      next;
    }
    if($last > $low) {
      push @ranges, $low . "-" . $last;
    } else {
      push @ranges, $low;
    }
    $low  = $high;
    $last = $low;
  }
  if(@sorted > 0 ){
      if($last > $low) {
          push @ranges, $low . "-" . $last;
      } else {
          push @ranges, $low;
      }
  }
  my $csv_vals = "";
  if(@vmnumbers > 0){
      $csv_vals = ",";
      $csv_vals .= join(",", @vmnumbers);
  }

  if(@ranges == 0 && $csv_vals ne ""){
      return  substr($csv_vals, 1, length($csv_vals));
  }

  # join the ranges with commas and return the compressed hostlist
  return $machine . "[" . join(",", @ranges) . "]" . $csv_vals;
}

# Returns a hostlist string given a list of hostnames
# compress('rhea2','rhea3','rhea4','rhea6') returns "rhea2,rhea3,rhea4,rhea6"
sub compress {
  if (@_ == 0) {
    return "";
  }

  # join nodes with commas
  return join(",", @_);
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
