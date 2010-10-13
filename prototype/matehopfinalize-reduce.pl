#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;
use Data::Dumper;

my $resolved_bundles = 0;
my $resolved_edges   = 0;
my $total_ambiguous  = 0;

sub finalize_node
{
  my $nodeid = shift;
  my $node = shift;

  return if !defined $nodeid;
  return if !defined $node;

  if (defined $node->{$BUNDLE})
  {
    my %paths;

    foreach my $b (@{$node->{$BUNDLE}})
    {
      if ($b =~ /^#/)
      {
        $b =~ s/^#//;
        my @path = split /:/, $b;

        my $startnode = $path[0];
        my $endnode = $path[scalar @path - 1];

        if ($endnode eq $nodeid)
        {
          ## Found a complete path to me
          push @{$paths{$startnode}->{paths}}, $b;
        }
        else
        {
          ## bundlemsg, skip
        }
      }
    }

    foreach my $startnode (keys %paths)
    {
      if (scalar @{$paths{$startnode}->{paths}} > 1)
      {
        # path is ambiguous
        $total_ambiguous++;
        next;
      }

      ## found a unique consistent path from startnode to nodeid

      my $path = $paths{$startnode}->{paths}->[0];

      my @hops = reverse split /:/, $path;

      my $curnode = shift @hops; ## nodeid
      my $curedge = flip_link($hops[0]);
      my $ut = substr($curedge,0,1);

      for(my $i = 0; $i < scalar @hops; $i+=2)
      {
        $curedge = flip_link($hops[$i]);
        $curnode = $hops[$i+1];

        push @{$node->{$MATETHREAD}}, "$ut:$curedge:$curnode";

        $resolved_edges++;
      }
    }

    $resolved_bundles++;
  }

  print_node($nodeid, $node);
}


while (<>)
{
  #print "==> $_";
  chomp;
  my @vals = split /\t/, $_;

  my $nodeid = shift @vals;
  my $msgtype = shift @vals;

  if ($msgtype ne $NODEMSG)
  {
    die "Unknown msg: $_\n";
  }

  my $node = {};
  parse_node($node, \@vals);

  finalize_node($nodeid, $node);
}


hadoop_counter("resolved_bundles", $resolved_bundles);
hadoop_counter("resolved_edges",   $resolved_edges);
hadoop_counter("total_ambiguous",  $total_ambiguous);
