#!/usr/bin/perl -w
use strict;

use File::Basename;
use lib ".";
use lib dirname($0);
use PAsm;

my $USAGE = "fixgraph.pl graphfile > fixed\n";

my $graphfile = shift @ARGV; die $USAGE if !defined $graphfile;

my %nodes;

open GRAPH, "< $graphfile" or die "Can't open $graphfile ($!)\n";

my $nodecnt = 0;

while (<GRAPH>)
{
  print $_;
  chomp;

  my $node = {};

  my @vals = split /\t/, $_;

  my $nodeid = shift @vals;

  my $msgtype = shift @vals; ## nodemsg

  if ($msgtype ne $NODEMSG)
  {
    die "Unknown msg: $_\n";
  }

  parse_node($node, \@vals);

  $nodes{$nodeid} = $node;
  $nodecnt++;

  if (($nodecnt % 1000) == 0)
  {
    print STDERR "Loaded $nodecnt\n";
  }
}

my $missingcnt = 0;
my %missing;

foreach my $nodeid (keys %nodes)
{
  my $node = $nodes{$nodeid};

  foreach my $t (qw/ff fr rr rf/)
  {
    if (exists $node->{$t} && scalar @{$node->{$t}})
    { 
      foreach my $b (@{$node->{$t}})
      { 
        if (!exists $nodes{$b})
        {
          $missingcnt++;

          my $tt = flip_link($t);

          push @{$missing{$b}->{$tt}}, $nodeid;
        }
      }
    }
  }
}

foreach my $mid (keys %missing)
{
  my $mn = $missing{$mid};

  node_setstr($mn, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
  $mn->{$COVERAGE}->[0] = 0;

  print_node($mid, $mn);
}

print STDERR "Total: $nodecnt added: $missingcnt\n";
