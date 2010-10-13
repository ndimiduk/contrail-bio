#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;
use Data::Dumper;

sub	updateNode
{
  my $nodeid = shift;
  my $node = shift;

  return if !defined $nodeid;

  #if ((exists $node->{update}) ||
  #    (exists $node->{compress}))
  #{
  #  print "updateNode: $nodeid\n";
  #  print Dumper($node);
  #  print "\n\n";
  #}

  if (exists $node->{update})
  {
    foreach my $up (@{$node->{update}})
    {
      node_replacelink($nodeid, $node,
                       $up->{oid}, $up->{odir},
                       $up->{nid}, $up->{ndir});
    }
  }

  print_node($nodeid, $node);
}

my $node = {};
my $nodeid = undef;

while (<>)
{
  #print "==> $_";
  chomp;
  my @vals = split /\t/, $_;

  my $curnodeid = shift @vals;
  if (defined $nodeid && $curnodeid ne $nodeid)
  {
    updateNode($nodeid, $node);
    $node = {};
  }

  $nodeid = $curnodeid;

  my $msgtype = shift @vals;

  if ($msgtype eq $NODEMSG)
  {
    parse_node($node, \@vals);
  }
  elsif ($msgtype eq $UPDATEMSG)
  {
    my $up;

    $up->{oid}  = shift @vals;
    $up->{odir} = shift @vals;
    $up->{nid}  = shift @vals;
    $up->{ndir} = shift @vals;

    push @{$node->{update}}, $up;
  }
  else
  {
    die "Unknown msg: $_\n";
  }
}

updateNode($nodeid, $node);

