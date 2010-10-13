#!/usr/bin/perl -w
use strict;

my $USAGE = "tagrepeats.pl coordsfile\n";

my $coordsfile = shift @ARGV or die $USAGE;

open COORDS, "< $coordsfile" or die "Can't open $coordsfile ($!)\n";

my %queries;
my $inheader = 1;
while (<COORDS>)
{
  if (/^================================/)
  {
    $inheader = 0;
    next;
  }

  next if $inheader;

  s/^\s+//;
  chomp;

  my @vals = split /\s+/, $_;

  my $qry = $vals[18];
  $queries{$qry}++;
  ##print "$qry\n";
}

close COORDS;



open COORDS, "< $coordsfile" or die "Can't open $coordsfile ($!)\n";

$inheader = 1;
while (<COORDS>)
{
  if (/^================================/)
  {
    $inheader = 0;
    next;
  }

  next if $inheader;

  chomp;

  my $orig = $_;

  s/^\s+//;

  my @vals = split /\s+/, $_;

  my $qry = $vals[18];
  my $cnt = $queries{$qry};

  print "$orig\t$cnt\n";
}




