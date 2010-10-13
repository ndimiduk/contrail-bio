#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;

my $threadible = 0;

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

  if (exists $node->{$THREADPATH})
  {
    $threadible++;

    ## Tell my neighbors that I intend to split
    foreach my $t (qw/ff fr rf rr/)
    {
      if (defined $node->{$t})
      {
        my $dir = flip_link($t);

        foreach my $v (@{$node->{$t}})
        {
          if ($v ne $nodeid)
          {
            print "$v\t$THREADIBLEMSG\t$dir:$nodeid\n";
          }
        }
      }
    }
  }
 
  print_node($nodeid, $node);
}

hadoop_counter("threadible", $threadible);




