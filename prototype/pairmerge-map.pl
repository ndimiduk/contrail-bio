#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;

while (<>)
{
  chomp;

  my $node = {};

  my @vals = split /\t/, $_;

  my $nodeid = shift @vals;

  my $msgtype = shift @vals; ## nodemsg

  if ($msgtype eq $NODEMSG)
  {
    parse_node($node, \@vals);
  }
  else
  {
    die "Unknown msg: $_\n";
  }

  my $mergedir = $node->{$MERGE}->[0];

  if (defined $mergedir)
  {
    my ($compressed, $compressbdir) = node_gettail($node, $mergedir);
    print "$compressed\t$COMPRESSPAIR\t$mergedir\t$compressbdir\t$_\n";
  }
  else
  {
    print "$_\n";
  }
}
