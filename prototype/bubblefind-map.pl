#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;

my $potentialbubbles = 0;

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

  print "$_\n"; ## reemit the node

  if (node_len($node) < $MAXBUBBLELEN)
  {
    my $fdegree = node_degree($node, "f");
    my $rdegree = node_degree($node, "r");

    if (($fdegree == 1) && ($rdegree == 1))
    {
      $potentialbubbles++;

      my ($fl,$fd) = node_gettail($node, "f");
      my ($rl,$rd) = node_gettail($node, "r");

      die "Couldn't find f or r neighbor\n"
       if !defined $fl || !defined $rl;

      my $major  = $fl;
      my $majord = "f$fd";

      my $minor  = $rl;
      my $minord = "r$rd";

      if ($rl gt $fl)
      {
        $major  = $rl;
        $majord = "r$rd";

        $minor  = $fl;
        $minord = "f$fd";
      }

      $majord = flip_link($majord);
      $minord = flip_link($minord);

      my $str = node_str_raw($node);
      my $cov = $node->{$COVERAGE}->[0];
      print "$major\t$BUBBLELINKMSG\t$majord\t$nodeid\t$minord\t$minor\t$str\t$cov\n";
    }
  }
}
 
hadoop_counter("potentialbubbles", $potentialbubbles);

