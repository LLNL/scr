package scr_meta;
use strict;

# create a new object, clear events hash
#  $updown = new $ibnetupdown($ibnetdiscover_file)
sub new
{
  my $type = shift;
  my $file = shift;

  my $self = {};

  return bless $self, $type;
}

# given a data file name, read its corresponding meta data file, a return a reference to a key/value hash
sub read
{
  my $self = shift;
  my $file = shift;

  my $ref = {};

  my $meta = $self->metaname($file);
  if (-r $meta) {
    open(IN, $meta);
    while (my $line = <IN>) {
      chomp $line;
      my ($key, $value) = split(/:\s+/, $line);
#print "$key --> $value\n";
      $$ref{$key} = $value;
    }
    close(IN);
  }

  return $ref;
}

# given a data file name, read its corresponding meta data file, a return a reference to a key/value hash
sub write
{
  my $self = shift;
  my $file = shift;
  my $ref  = shift;

  my $meta = $self->metaname($file);
  open(OUT, ">$meta");
  foreach my $key (sort {$a cmp $b} keys %$ref) {
    my $value = $$ref{$key};
#print "$key --> $value\n";
    print OUT "$key: $value\n";
  }
  close(OUT);

  return $ref;
}

sub metaname
{
  my $self = shift;
  my $file = shift;
  my $meta = "$file.scr";
#print "File: $file --> Meta: $meta\n";
  return $meta;
}

1;
