package scr_hash;
use strict;

# reads in an SCR hash file and translates it
# into a perl hash object

sub new
{
  my $type = shift;
  my $prog = shift;

  my $self = {};

  $self->{prog} = $prog;
  $self->{file} = undef;

  return bless $self, $type;
}

sub read_file
{
  my $self = shift @_;
  my $file = shift @_;
  my $h    = shift @_;

  $self->{file} = $file;

  my $rc = 1;
  if (open(IN, $file)) {
    # read the "Start" marker
    my $line = <IN>;
    if ($rc and not defined $line or $line !~ /Start/) {
      print "$self->{prog}: $self->{file}: Missing 'Start' marker in hashfile\n";
      $rc = 0;
    }

    # read the hash data
    if ($rc and not $self->read_hash($h)) {
      print "$self->{prog}: $self->{file}: Failed to read hash\n";
      $rc = 0;
    }

    # read the "End" marker
    if ($rc) {
      $line = <IN>;
      if (not defined $line or $line !~ /End/) {
        print "$self->{prog}: $self->{file}: Missing 'End' marker in hashfile\n";
        $rc = 0;
      }
    }

    # done reading, close the file
    close(IN);
  } else {
    print "$self->{prog}: $self->{file}: Could not open file for reading\n";
    $rc = 0;
  }

  return $rc;
}

sub read_hash
{
  my $self = shift;
  my $h    = shift;

  my $rc = 1;

  # read in the count field
  my $line = <IN>;
  if (defined $line and $line =~ /C:(\d+)/) {
    my $count = $1;

    # now, read in each element
    for (my $i = 0; $i < $count; $i++) {
      if (not $self->read_elem($h)) {
        print "$self->{prog}: $self->{file}: Invalid read of hash elements\n";
        $rc = 0;
        last;
      }
    }
  } else {
    print "$self->{prog}: $self->{file}: Missing count field in hash file\n";
    $rc = 0;
  }

  return $rc;
}

sub read_elem
{
  my $self = shift;
  my $h    = shift;

  my $rc = 1;

  # read in the key
  my $key = <IN>;
  if (defined $key) {
    chomp $key;

    # if this key is not already defined, create a hash for this key
    if (not defined $$h{$key}) {
      %{$$h{$key}} = ();
    }

    # read in the hash data for this key
    $rc = $self->read_hash(\%{$$h{$key}});
  } else {
    print "$self->{prog}: $self->{file}: Missing key in hash file\n";
    $rc = 0;
  }
  return $rc;
}

sub write_file
{
  my $self = shift @_;
  my $file = shift @_;
  my $h    = shift @_;

  $self->{file} = $file;

  my $rc = 1;
  if (open(OUT, ">$file")) {
    # write the "Start" marker
    print OUT "Start\n";

    # write the hash data
    $self->write_hash($h);

    # write the "End" marker
    print OUT "End\n";

    # done writing, close the file
    close(IN);
  } else {
    print "$self->{prog}: Could not open file for writing: $self->{file}\n";
    $rc = 0;
  }

  return $rc;
}

sub write_hash
{
  my $self = shift;
  my $h    = shift;

  # get the keys of this hash
  my @keys = (keys %$h);

  # write the element count
  my $count = scalar(@keys);
  print OUT "C:$count\n";

  # write out each element
  foreach my $key (@keys) {
    # write the key name
    print OUT "$key\n";

    # write the hash for this key
    if (ref($$h{$key}) eq "HASH") {
      $self->write_hash(\%{$$h{$key}});
    } else {
      print OUT "C:0\n";
    }
  }
}

1;
