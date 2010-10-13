#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;

while (<>)
{
  print $_;

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

  ## Tell all my neighbors that I intend to compress
  if ($node->{"$CANCOMPRESS$fwd"}->[0] ||
      $node->{"$CANCOMPRESS$rev"}->[0])
  {
    foreach my $t (qw/ff fr rf rr/)
    {
      if (exists $node->{$t})
      {
        foreach my $v (@{$node->{$t}})
        {
          print "$v\t$COMPRESSPAIR\n";
        }
      }
    }
  }
}
