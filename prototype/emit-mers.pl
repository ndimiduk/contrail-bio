#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;

my $BATCHSIZE = 10000000;
my $READ_PREFIX = "";

if (exists $ENV{READ_PREFIX})
{
  $READ_PREFIX = $ENV{READ_PREFIX};
  print STDERR "read_prefix: $READ_PREFIX\n";
}


my %stats;
my %mertable;

while(<>)
{
  $stats{lines_total}++;

  if    (/^\@$READ_PREFIX/) { $stats{reads_total}++; next; }
  elsif (/^\+$READ_PREFIX/) { $stats{qual_header}++; next; }

  chomp;

  if (/[^ACGTN\.]/)
  {
    $stats{qual_string}++;
    next;
  }

  my $seq = uc($_);

  ## Automatically trim Ns off the very ends of reads
  $seq =~ s/^N+//;
  $seq =~ s/N+$//;

  my $l = length $seq;
  my $end = $l - $K + 1;

  ## check for short reads
  if ($l < $K)
  {
    $stats{reads_short}++;
    next;
  }

  ## skip reads with remaining N's
  if ($seq =~ /N/)
  {
    $stats{reads_n}++;
    next;
  }

  ## read is good, emit mers
  $stats{reads_good}++;
  $stats{reads_goodbp} += $l;

  for (my $i = 0; $i < $end; $i++)
  {
    $stats{mers_counted}++;

    my $u = substr($seq, $i, $K);
    my ($uc, $ud) = canonical($u);
    $uc = str2dna($uc);

    $mertable{$uc}++;
  }

  ## See if the mers should be printed
  if (scalar keys %mertable >= $BATCHSIZE)
  {
    while (my ($mer, $cnt) = each %mertable)
    {
      print "$mer\t$cnt\n";
      $stats{mers_emited}++;
    }

    undef %mertable;

    foreach my $tag (sort keys %stats)
    {
      my $val = $stats{$tag};
      print STDERR "reporter:counter:asm,$tag,$val\n";
      print STDERR "stats: $tag $val\n";
      $stats{$tag} = 0;
    }

    print STDERR "reporter:counter:asm,flush,1\n";
  }
}

while (my ($mer, $cnt) = each %mertable)
{
  print "$mer\t$cnt\n";
  $stats{mers_emited}++;
}

undef %mertable;

foreach my $tag (sort keys %stats)
{
  my $val = $stats{$tag};
  print STDERR "reporter:counter:asm,$tag,$val\n";
  print STDERR "stats: $tag $val\n";
  $stats{$tag} = 0;
}

print STDERR "reporter:counter:asm,flush,1\n";
