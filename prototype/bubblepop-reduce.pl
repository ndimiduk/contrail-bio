#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;

my $linksremoved = 0;
my $threadsremoved = 0;

sub	popbubbles
{
  my $nodeid = shift;
  my $node = shift;

  return if !defined $nodeid;

  ## Just remove the popped nodes
  return if defined $node->{"kill"};

  if (exists $node->{$EXTRACOV})
  {
    my $merlen = node_len($node) - $K + 1;
    my $support = $node->{$COVERAGE}->[0] * $merlen + $node->{$EXTRACOV};

    $node->{$COVERAGE}->[0] = $support / $merlen;
  }

  if (exists $node->{"killlink"})
  {
    foreach my $killd (keys %{$node->{killlink}})
    {
      foreach my $dead (keys %{$node->{killlink}->{$killd}})
      {
        $linksremoved++;
        node_removelink($node, $dead, $killd);
      }
    }

    if (exists $node->{$THREAD})
    {
      foreach my $thread (@{$node->{$THREAD}})
      {
        my ($t,$link,$read) = split /:/, $thread;

        if (exists $node->{killlink}->{$t}->{$link})
        {
          my $newport = $node->{killlink}->{$t}->{$link};
          $thread = "$newport:$link";
        }
      }
    }

    $threadsremoved += node_cleanthreads($node);
  }

  print_node($nodeid, $node);
}

my $node = {};
my $nodeid = undef;

while (<>)
{
 # print "==> $_";
  chomp;
  my @vals = split /\t/, $_;

  my $curnodeid = shift @vals;
  if (defined $nodeid && $curnodeid ne $nodeid)
  {
    popbubbles($nodeid, $node);
    $node = {};
  }

  $nodeid = $curnodeid;

  my $msgtype = shift @vals;

  if ($msgtype eq $NODEMSG)
  {
    parse_node($node, \@vals);
  }
  elsif ($msgtype eq $KILLLINKMSG)
  {
    my $deadd = shift @vals;
    my $dead  = shift @vals;
    my $newd  = shift @vals;
    my $new   = shift @vals;
    $node->{killlink}->{$deadd}->{$dead} = "$newd:$new";
  }
  elsif ($msgtype eq $KILLMSG)
  {
    $node->{kill} = 1;
  }
  elsif ($msgtype eq $EXTRACOV)
  {
    my $extracov = shift @vals;
    $node->{$EXTRACOV} += $extracov;
  }
  else
  {
    die "Unknown msg: $_\n";
  }
}

popbubbles($nodeid, $node);

hadoop_counter("removed_threads", $threadsremoved);
hadoop_counter("removed_links", $linksremoved);
