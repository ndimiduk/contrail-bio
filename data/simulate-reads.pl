#!/usr/bin/perl -w
use strict;
use Getopt::Long;

my $readlen = 36;
my $insertlen = 0;
my $help = 0;

my $result = GetOptions(
  "h"    => \$help,
  "I=s"  => \$insertlen,
  "r=s"  => \$readlen,
);

if (!$result || $help)
{
  print "simulate-reads.pl [-r readlen] [-I insertlen]\n";
  exit 0;
}

sub rc
{
  my $sequence = reverse $_[0];
  $sequence =~ tr/GATC/CTAG/;
  return $sequence;
}

my $seq;
while (<>)
{
  if (/^>/)
  {
    next;
  }
  
  chomp;
  $seq .= $_;
}


my $len = length ($seq);

my $end = $len - $readlen + 1;

if ($insertlen)
{
  $end = $len - $insertlen + 1;
}

my $id = 1;
my $i;
for ($i = 0; $i < $end; $i++, $id++)
{
  if ($insertlen)
  {
    my $r1o = $i;
    my $r2o = $i+$insertlen-$readlen;

    my $r1 = substr($seq, $r1o, $readlen);
    my $r2 = rc(substr($seq, $r2o, $readlen));

    print ">$id\_1 $r1o\n$r1\n>$id\_2 $r2o\n$r2\n";
  }
  else
  {
    my $rs = substr($seq, $i, $readlen);
    print ">$id\n$rs\n";
  }
}

