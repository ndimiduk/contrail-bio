#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;

while (<>)
{
  print $_;

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
  
  if (exists $node->{$THREAD})
  {
    my %edges;

    ## Index the edges that exist
    foreach my $t (qw/ff fr rf rr/)
    {
      if (exists $node->{$t})
      {
        foreach my $v (@{$node->{$t}})
        {
          next if $v eq $nodeid;

          $edges{$t}->{$v} = 1;
        }
      }
    }

    my %threads;

    ## Index the threading reads
    foreach my $thread (@{$node->{$THREAD}})
    {
      my ($t,$link,$read) = split /:/, $thread;

      if (exists $edges{$t}->{$link})
      {
        push @{$threads{$t}->{$link}}, $read;
      }
    }

    foreach my $tdir (keys %threads)
    {
      foreach my $link (keys %{$threads{$tdir}})
      {
        my $f = flip_link($tdir);
        print "$link\t$UPDATEMSG\t$f\t$nodeid\t", 
              join("\t", @{$threads{$tdir}->{$link}}), "\n";
      }
    }
  }
}

