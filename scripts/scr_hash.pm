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

sub read_hash_file
{
  my $self = shift @_;
  my $file = shift @_;

  my $h = undef;

  $self->{file} = $file;

  if (open(IN, $file)) {
    $h = $self->read_hash();

    # read in the End marker
    my $line = <IN>;
    if (not defined $line or $line !~ /End/) {
      print "$self->{prog}: Missing 'End' marker in hashfile: $file\n";
      $h = undef;
    }

    # done reading, close the file
    close(IN);
  } else {
    print "$self->{prog}: Could not open file: $self->{file}\n";
  }

  return $h;
}

sub read_hash
{
  my $self = shift;
  my $h = {};

  my $line = <IN>;
  if (defined $line and $line =~ /C:(\d+)/) {
   my $count = $1;
   for (my $i = 0; $i < $count; $i++) {
     my ($name, $hash) = $self->read_hash_elem();
     if (defined $name and defined $hash) {
       %{$$h{$name}} = %$hash;
     } else {
       print "Invalid read of hash elements\n";
       $h = undef;
     }
   }
  } else {
    print "$self->{prog}: $self->{file}: Missing count field in hash file\n";
    $h = undef;
  }

  return $h;
}

sub read_hash_elem
{
  my $self = shift;
  my $line = <IN>;
  if (defined $line) {
    chomp $line;
    my $h = $self->read_hash();
    return ($line, $h);
  } else {
    print "$self->{prog}: $self->{file}: Missing key in hash file\n";
  }
  return (undef, undef);
}

1;
