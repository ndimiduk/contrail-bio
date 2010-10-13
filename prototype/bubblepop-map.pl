#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;

my $bubblespopped = 0;

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


  if (defined $node->{$POPBUBBLE})
  {
    foreach my $bubble (@{$node->{$POPBUBBLE}})
    {
      my ($minor, $minord, $dead, $newd, $new, $extracov) = split /\|/, $bubble;

      print "$minor\t$KILLLINKMSG\t$minord\t$dead\t$newd\t$new\n";
      print "$dead\t$KILLMSG\n";
      print "$new\t$EXTRACOV\t$extracov\n";

      $bubblespopped++;
    }

    $node->{$POPBUBBLE} = undef;
    print_node($nodeid, $node);
  }
  else
  {
    print "$_\n";
  }
}
 
hadoop_counter("bubblespopped", $bubblespopped);


