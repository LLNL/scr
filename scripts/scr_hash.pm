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

sub readstr
{
  my $n = undef;
  my $c = 0;

  $n = read(IN, $c, 1);
  if (not defined $n or $n == 0) {
    return undef;
  }

  my @chars = ();
  while (ord($c) != 0) {
    push @chars, $c;
    $n = read(IN, $c, 1);
    if (not defined $n or $n == 0) {
      return undef;
    }
  }

  my $str = join("", @chars);
#print "$str\n";
  return $str;
}

sub readN
{
  # get the number of bytes we should read
  my $count = shift @_;

  my $val = undef;
  if (defined $count and $count > 0) {
    $val = 0;
    while ($count > 0) {
      $count--;
      $val *= 256;

      # read in a byte and check that the read succeeded
      my $c = "";
      my $n = read(IN, $c, 1);
      if (not defined $n or $n == 0) {
        return undef;
      }

      $val += ord($c);
    }
  }

  return $val;
}

sub read16
{
  my $val = readN(2);
#printf("%lu (%0x)\n", $val, $val);
  return $val;
}

sub read32
{
  my $val = readN(4);
#printf("%lu (%0x)\n", $val, $val);
  return $val;
}

sub read64
{
  my $val = readN(8);
#printf("%lu (%0x)\n", $val, $val);
  return $val;
}

sub read_file
{
  my $self = shift @_;
  my $file = shift @_;
  my $h    = shift @_;

  $self->{file} = $file;

  my $rc = 1;
  if (open(IN, $file)) {
    # set file to binary mode
    binmode IN;

    # check that the magic number matches
    my $magic = read32();
    if (not defined $magic or $magic != 0x951fc3f5) {
      print "$self->{prog}: $self->{file}: Unrecognized magic number\n";
      $rc = 0;
    }

    # check that the file type matches
    my $type = read16();
    if (not defined $type or $type != 1) {
      print "$self->{prog}: $self->{file}: Unsupported file type: $type\n";
      $rc = 0;
    }

    # check that the file version matches
    my $version = read16();
    if (not defined $version or $version != 1) {
      print "$self->{prog}: $self->{file}: Unsupported file type / version: $type / $version\n";
      $rc = 0;
    }

    # check that the filesize matches
    my $size = read64();
    my $true_size = (stat $file)[7];
    if (not defined $size or ($size != $true_size)) {
      print "$self->{prog}: $self->{file}: Incorrect file size, expected $size but actual is $true_size bytes\n";
      $rc = 0;
    }

    # check that the magic number matches
    my $flags = read32();
    if (not defined $flags or $flags != 0x00000001) {
      print "$self->{prog}: $self->{file}: Unrecognized flags field\n";
      $rc = 0;
    }

    # read the hash data
    if ($rc and not $self->read_hash($h)) {
      print "$self->{prog}: $self->{file}: Failed to read hash\n";
      $rc = 0;
    }

    # read the crc
    # TODO: check that the crc matches
    my $crc = read32();
    if (not defined $crc) {
      print "$self->{prog}: $self->{file}: Failed to read CRC32 value\n";
      $rc = 0;
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
  my $count = read32();
  if (defined $count) {
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
  my $key = readstr();
  if (defined $key) {
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

######################################
### reads were used longer than writes
### so the write code below may not be
### consistent with read code above
######################################

#sub writestr
#{
#  my $str = shift @_;
#  my $len = scalar($str);
#
#  # write out the string characters
#  my $n = write(OUT, $str, $len);
#  if (not defined $n or $n != $len) {
#    return undef;
#  }
#
#  # null-terminate the string
#  my $c = '\0';
#  my $rc = $write(OUT, $c, 1);
#  if (not defined $rc or $rc != 1) {
#    return undef;
#  }
#
#  my $nwrite = $len + 1;
#  return $nwrite;
#}
#
#sub writeN
#{
#  # get the number of bytes we should read
#  my $count = shift @_;
#  my $val = shift @_;
#
#  my $count_ret = undef;
#  if (defined $count and defined $val and $count > 0) {
#    my $count_ret = $count;
#    while ($count > 0) {
#      $count--;
#
#      # compute the denominator for this byte
#      my $div = 1;
#      for (my $i = 0; $i < $count; $i++) {
#        $div *= 256;
#      }
#
#      # compute the value of the byte
#      my $c = chr(int($val / $div));
#printf("%lu (%0x)\n", $c, $c);
#
#      # write the byte
#      my $n = write(OUT, $c, 1);
#      if (not defined $n or $n == 0) {
#        return undef;
#      }
#
#      # subtract this piece from val
#      $val -= $c * $div;
#    }
#  }
#
#  return $count_ret;
#}
#
#sub write16
#{
#  my $val = shift @_;
#  my $ret = writeN(2, $val);
#  return $val;
#}
#
#sub write32
#{
#  my $val = shift @_;
#  my $ret = writeN(4, $val);
#  return $val;
#}
#
#sub write64
#{
#  my $val = shift @_;
#  my $ret = writeN(8, $val);
#  return $val;
#}
#
#sub write_file
#{
#  my $self = shift @_;
#  my $file = shift @_;
#  my $h    = shift @_;
#
#  $self->{file} = $file;
#
#  my $rc = 1;
#  if (open(OUT, ">$file")) {
#    # set file to binary mode
#    binmode IN;
#
#    my $tmp_rc = undef;
#
#    # write the magic number
#    $tmp_rc = write32(0x951fc3f5);
#    if (not defined $tmp_rc or $tmp_rc != 4) {
#      print "$self->{prog}: $self->{file}: Failed to write magic number\n";
#      $rc = 0;
#    }
#
#    # write the file type
#    $tmp_rc = write16(1);
#    if (not defined $tmp_rc or $tmp_rc != 2) {
#      print "$self->{prog}: $self->{file}: Failed to write file type\n";
#      $rc = 0;
#    }
#    
#    # write the file type version
#    $tmp_rc = write16(1);
#    if (not defined $tmp_rc or $tmp_rc != 2) {
#      print "$self->{prog}: $self->{file}: Failed to write file type version\n";
#      $rc = 0;
#    }
#    
#    # write the file size (just write 0 for now, we come back and fill this in later)
#    $tmp_rc = write64(0);
#    if (not defined $tmp_rc or $tmp_rc != 8) {
#      print "$self->{prog}: $self->{file}: Failed to write file size\n";
#      $rc = 0;
#    }
#
#    # write the hash data
#    if ($rc and not $self->write_hash($h)) {
#      print "$self->{prog}: $self->{file}: Failed to write hash\n";
#      $rc = 0;
#    }
#
#    close(OUT);
#
#    # open file and write file size
#    open(OUT, ">>$file");
#    my $size = (stat $file)[7];
#    my $size += 4; # add four for the crc we'll tack on the end
#    seek(OUT, 8, 0);
#    $tmp_rc = write64($size);
#    if (not defined $tmp_rc or $tmp_rc != 8) {
#      print "$self->{prog}: $self->{file}: Failed to write file size\n";
#      $rc = 0;
#    }
#    close(OUT);
#
#    # TODO: compute crc and append to file
#    # done reading, close the file
#    open(OUT, ">>$file");
#    my $crc_str = `$root/bin/scr_crc`;
#    $tmp_rc = write32($crc);
#    if (not defined $tmp_rc or $tmp_rc != 4) {
#      print "$self->{prog}: $self->{file}: Failed to write CRC32\n";
#      $rc = 0;
#    }
#    close(OUT);
#  } else {
#    print "$self->{prog}: $self->{file}: Could not open file for writing\n";
#    $rc = 0;
#  }
#
#  return $rc;
#}
#
#sub write_hash
#{
#  my $self = shift;
#  my $h    = shift;
#
#  my $n = undef;
#
#  # get the keys of this hash
#  my @keys = (keys %$h);
#
#  # write the element count
#  my $count = scalar(@keys);
#  $n = write64($count);
#  if (not defined $n or $n != 8) {
#    return undef;
#  }
#
#  # write out each element
#  foreach my $key (@keys) {
#    # write the key name
#    $n = writestr($key);
#    if (not defined $n) {
#      return undef;
#    }
#
#    # write the hash for this key
#    if (ref($$h{$key}) eq "HASH") {
#      $self->write_hash(\%{$$h{$key}});
#    } else {
#      $n = write64(0);
#      if (not defined $n or $n != 8) {
#        return undef;
#      }
#    }
#  }
#}

1;
