#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;
use Data::Dumper;

my $lowcovremoved = 0;

print STDERR "Removing nodes <= $MAX_LOW_COV_LEN bp with cov <= $MAX_LOW_COV_THRESH\n";

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

  my $len = node_len($node);
  my $cov = node_cov($node);

  if (($len <= $MAX_LOW_COV_LEN) && ($cov <= $MAX_LOW_COV_THRESH))
  {
    #print STDERR "Deleting low coverage node $nodeid len=$len cov=$cov\n";
    $lowcovremoved++;

    foreach my $et (qw/ff fr rf rr/)
    {
      if (exists $node->{$et})
      {
        my $ret = flip_link($et);

        foreach my $v (@{$node->{$et}})
        {
          print "$v\t$TRIMMSG\t$ret\t$nodeid\n";
        }
      }
    }
  }
  else
  {
    print "$_\n";
  }
}
 
hadoop_counter("lowcovremoved", $lowcovremoved);

