#!/usr/bin/perl -w
use strict;

use lib ".";
use PAsm;

my $matethreadmsgs = 0;

sub print_hopmsg
{
 my ($path, $curdist, $outdir, $expdist, $expdir, $dest, $node) = @_;

  ## Looking for a path to $ctg in the ud direction
  foreach my $curdir (qw/f r/)
  {
    my $tt = "$outdir$curdir";
    if (exists $node->{$tt})
    {
      foreach my $v (@{$node->{$tt}})
      {
        print "$v\t$MATETHREAD\t$path:$tt\t$curdist\t$curdir\t$expdist\t$expdir\t$dest\n";

        $matethreadmsgs++;
      }
    }
  }
}


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
    die "Unknown msg: $msgtype $_\n";
  }

  if ($FIRST_HOP)
  {
    print "$_\n";

    if (exists $node->{$BUNDLE})
    {
      my $curdist = -$K+1;

      foreach my $b (@{$node->{$BUNDLE}})
      {
        my ($dest,$edgetype,$expdist,$weight,$unique) = split /:/, $b;

        my $outdir = substr($edgetype, 0, 1);
        my $expdir = substr($edgetype, 1, 1);

        print_hopmsg($nodeid, 
                     $curdist, $outdir, 
                     $expdist, $expdir, 
                     $dest, 
                     $node);
      }
    }
  }
  else
  {
    if (exists $node->{$MATETHREAD})
    {
      foreach my $hopmsg (@{$node->{$MATETHREAD}})
      {
        my ($path, $curdist, $outdir, $expdist, $expdir, $dest) = 
          split /\%/, $hopmsg;

        print_hopmsg($path, 
                     $curdist, $outdir, 
                     $expdist, $expdir, 
                     $dest, 
                     $node);
      }

      delete $node->{$MATETHREAD};
    }

    print_node($nodeid, $node);
  }
}

hadoop_counter("matethreadmsgs", $matethreadmsgs);
