#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;
use Data::Dumper;

my $all_bundles    = 0;
my $unique_bundles = 0;

my $V = 0;

sub	bundleEdges
{
  my $nodeid = shift;
  my $node = shift;

  return if !defined $nodeid;

  if (exists $node->{edges})
  {
    if (node_isunique($node))
    {
      foreach my $edgetype (keys %{$node->{edges}})
      {
        foreach my $ctg (keys %{$node->{edges}->{$edgetype}})
        {
          ## TODO: cluster consistent distances


          ## For now bundle all edges to contig
          my $weight = scalar @{$node->{edges}->{$edgetype}->{$ctg}};

          my $unique = $node->{edges}->{$edgetype}->{$ctg}->[0]->{unique};
          $all_bundles++;

          if ($unique)
          {
            $unique_bundles++;

            my $sum = 0;
            foreach my $e (@{$node->{edges}->{$edgetype}->{$ctg}}) 
            { $sum += $e->{dist}; }

            my $dist = int($sum / $weight);
            my $bstr = "$ctg:$edgetype:$dist:$weight:$unique";

            if ($V) { print STDERR "Bundle $nodeid $bstr\n"; }
            push @{$node->{$BUNDLE}}, $bstr;
          }
        }
      }
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
    bundleEdges($nodeid, $node);
    $node = {};
  }

  $nodeid = $curnodeid;

  my $msgtype = shift @vals;

  if ($msgtype eq $NODEMSG)
  {
    parse_node($node, \@vals);
  }
  elsif ($msgtype eq $MATEEDGE)
  {
    my $edgetype = shift @vals;
    my $ctg      = shift @vals;

    my $edge;
    $edge->{dist}     = shift @vals;
    $edge->{basename} = shift @vals;
    $edge->{unique}   = shift @vals;

    push @{$node->{edges}->{$edgetype}->{$ctg}}, $edge;
  }
  else
  {
    die "Unknown msg: $_\n";
  }
}

bundleEdges($nodeid, $node);

hadoop_counter("unique_bundles", $unique_bundles);
