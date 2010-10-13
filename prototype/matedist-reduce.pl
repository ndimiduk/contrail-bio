#!/usr/bin/perl -w
use strict;

use lib ".";
use PAsm;

my $linking_edges = 0;


my $basename1;
my $ctg1;
my $read1;
my $rc1;
my $dist1;
my $unique1;

while (<>)
{
  chomp;

  my @vals = split /\t/, $_;

  my $basename = shift @vals;

  my $msgtype = shift @vals;

  if ($msgtype ne $MATEDIST)
  {
    die "Unknown msg: $_\n";
  }

  if ((!defined $basename1) || ($basename ne $basename1))
  {
    $basename1 = $basename;

    $ctg1    = shift @vals;
    $read1   = shift @vals;
    $rc1     = shift @vals;
    $dist1   = shift @vals;
    $unique1 = shift @vals;
  }
  else
  {
    my $ctg2    = shift @vals;
    my $read2   = shift @vals;
    my $rc2     = shift @vals;
    my $dist2   = shift @vals;
    my $unique2 = shift @vals;

    ## Don't both record repeat-repeat bundles
    if ($unique1 || $unique2)
    {
      my $insertlen = mate_insertlen($read1, $read2);

      my $dist = $insertlen - $dist1 - $dist2;

      my $ee1 = $rc1 ? "r" : "f";
      my $ee2 = $rc2 ? "f" : "r"; 

      my $ee = "$ee1$ee2";
      my $ff = flip_link($ee);

      $linking_edges++;

      print "$ctg1\t$MATEEDGE\t$ee\t$ctg2\t$dist\t$basename\t$unique2\n";
      print "$ctg2\t$MATEEDGE\t$ff\t$ctg1\t$dist\t$basename\t$unique1\n";
    }
  }
}


hadoop_counter("linking_edges", $linking_edges);

