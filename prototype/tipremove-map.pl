#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;
use Data::Dumper;

my $tipsremoved = 0;

print STDERR "Removing tips <= $TIPLENGTH bp\n";

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

  my $fdegree = node_degree($node, "f");
  my $rdegree = node_degree($node, "r");
  my $len     = node_len($node);

  if (($len <= $TIPLENGTH) && ($fdegree + $rdegree <= 1))
  {
    $tipsremoved++;

    #print STDERR "Removing tip $nodeid len = $len $_\n";

    if (($fdegree == 0) && ($rdegree == 0))
    {
      ## this node is not connected to the rest of the graph
      ## nothing to do
    }
    else
    {
      ## Tell the one neighbor that I'm a tip
      my $linkdir = ($fdegree == 0) ? "r" : "f";

      foreach my $adj (qw/f r/)
      {
        my $key = "$linkdir$adj";
        if (exists $node->{$key})
        {
          my $len = scalar @{$node->{$key}};

          if ($len != 1)
          {
            print STDERR Dumper($node);
            die "Expected a single $linkdir connection from $nodeid $len $fdegree $rdegree";
          }
          
          my $p = $node->{$key}->[0];

          if ($p eq $nodeid)
          {
            ## short tandem repeat, trim away
          }
          else
          {
            my $con = flip_dir($adj) . flip_dir($linkdir);
            print "$p\t$TRIMMSG\t$con\t$_\n";
          }
        }
      }
    }
  }
  else
  {
    print "$_\n";
  }
}
 
hadoop_counter("tipsremoved", $tipsremoved);

