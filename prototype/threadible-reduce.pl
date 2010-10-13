#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;

sub markThreadible
{
  my $nodeid = shift;
  my $node = shift;

  return if !defined $nodeid;

  if (defined $node->{threadible})
  {
    foreach my $port (@{$node->{threadible}})
    {
      push @{$node->{$THREADIBLEMSG}}, $port;
    }
  }

  print_node($nodeid, $node);
}


my $node = {};
my $nodeid = undef;

while (<>)
{
  chomp;

  my @vals = split /\t/, $_;

  my $curnodeid = shift @vals;
  if (defined $nodeid && $curnodeid ne $nodeid)
  {
    markThreadible($nodeid, $node);
    $node = {};
  }

  $nodeid = $curnodeid;

  my $msgtype = shift @vals;

  if ($msgtype eq $NODEMSG)
  {
    parse_node($node, \@vals);
  }
  elsif ($msgtype eq $THREADIBLEMSG)
  {
    my $port = shift @vals;
    push @{$node->{threadible}}, $port;
  }
  else
  {
    die "Unknown msg: $_\n";
  }
}

markThreadible($nodeid, $node);
