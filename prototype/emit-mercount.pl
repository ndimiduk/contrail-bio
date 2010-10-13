#!/usr/bin/perl -w
use strict;

my $BATCHSIZE = 10000000;

my %freqhist;

while (<>)
{
  chomp;
  my ($mer,$freq) = split /\s+/;

  $freqhist{$freq}++;

  if (scalar keys %freqhist >= $BATCHSIZE)
  {
    while (my ($freq, $cnt) = each %freqhist)
    {
      print "$freq\t$cnt\n";
    }

    undef %freqhist;
    print STDERR "reporter:counter:asm,flush,1\n";
  }
}

while (my ($freq, $cnt) = each %freqhist)
{
  print "$freq\t$cnt\n";
}

print STDERR "reporter:counter:asm,flush,1\n";
