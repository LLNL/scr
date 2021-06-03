#! /usr/bin/env python

# scr_list_dir.py

# This script returns info on the SCR control, cache, and prefix directories
# for the current user and jobid, it returns "INVALID" if something
# is not set.

# Better to have this directory construction in one place
# rather than having it duplicated over a number of different
# scripts

from scr_param import SCR_Param
param = new SCR_Param()

# TODO: read cache directory from config file
prog = "scr_list_dir"

bindir = "@X_BINDIR@"

def print_usage():
  print('')
  print('Usage:  '+prog+' [options] <control | cache>')
  print('')
  print('  Options:')
  print('    -u, --user     Specify username.')
  print('    -j, --jobid    Specify jobid.')
  print('    -b, --base     List base portion of cache/control directory')
  print('')
  sys.exit(1)

# read in command line arguments
conf = {}
conf[user]   = None
conf[jobid]  = None
conf[base]   = None
rc = GetOptions (
   "user|u=s"  => \$conf{user},
   "jobid|j=i" => \$conf{jobid},
   "base|b"    => \$conf{base},
);
if (not $rc) {
  print_usage();
}

# should have exactly one argument
if (@ARGV != 1) {
  print_usage();
}

# check that user specified "control" or "cache"
my $dir = shift @ARGV;
if ($dir ne "control" and $dir ne "cache") {
  print_usage();
}

# get the base directory
my @bases = ();
if ($dir eq "cache") {
  # lookup cache base
  my $cachedesc = $param->get_hash("CACHE");
  if (defined $cachedesc) {
    foreach my $index (keys %$cachedesc) {
      push @bases, $index;
    }
  }
} else {
  # lookup cntl base
  push @bases, $param->get("SCR_CNTL_BASE");
}
if (@bases == 0) {
  print "INVALID\n";
  exit 1;
}

# get the user/job directory
my $suffix = undef;
if (not defined $conf{base}) {
  # if not specified, read username from environment
  if (not defined $conf{user}) {
    my $username = `$bindir/scr_env --user`;
    if ($? == 0) {
      chomp $username;
      $conf{user} = $username;
    }
  }

  # if not specified, read jobid from environment
  if (not defined $conf{jobid}) {
    my $jobid = `$bindir/scr_env --jobid`;
    if ($? == 0) {
      chomp $jobid;
      $conf{jobid} = $jobid;
    }
  }

  # check that the required environment variables are set
  if (not defined $conf{user} or
      not defined $conf{jobid})
  {
    # something is missnig, print invalid dir and exit with error
    print "INVALID\n";
    exit 1;
  }

  $suffix = "$conf{user}/scr.$conf{jobid}";
}

# ok, all values are here, print out the directory name and exit with success
my @dirs = ();
foreach my $base (@bases) {
  if (defined $suffix) {
    push @dirs, "$base/$suffix";
  } else {
    push @dirs, "$base";
  }
}
if (@dirs > 0) {
  print join(" ", @dirs), "\n";
}
exit 0;
