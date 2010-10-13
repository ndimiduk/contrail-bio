#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;

my $V = 0;

my %nodes;

sub resolveThreads
{
  my $nodeid;
  my $node;

  while (($nodeid, $node) = each %nodes)
  {
    next if (!defined $node->{$THREAD});

    my %reads;
    my %pairs;

    ## Index the threading reads
    foreach my $thread (@{$node->{$THREAD}})
    {
      my ($t,$link,$read) = split /:/, $thread;
      push @{$reads{$read}}, "$t:$link";
    }

    ## Index the pairs
    foreach my $read (keys %reads)
    {
      if (scalar @{$reads{$read}} == 2)
      {
        my $l = join("-", sort @{$reads{$read}});

        push @{$pairs{$l}}, $read;
      }
    }

    my %portstatus;

    my $str = node_str($node);

    ## Now unzip
    my $copy = 0;
    foreach my $pt (keys %pairs)
    {
      $copy++;

      print STDERR "$pt : ", join(",", @{$pairs{$pt}}), "\n";

      my ($aport, $bport) = split /-/, $pt;

      my ($at, $alink) = split /:/, $aport;
      my ($bt, $blink) = split /:/, $bport;

      my $newnodeid = "$nodeid.$copy";

      my $newnode;
      $newnode->{$at}->[0] = $alink;
      $newnode->{$bt}->[0] = $blink;

      node_setstr($newnode, $str);

      push @{$nodes{$alink}->{flip_link($at)}}, $newnodeid;
      push @{$nodes{$blink}->{flip_link($bt)}}, $newnodeid;

      $portstatus{$aport} = 1;
      $portstatus{$bport} = 1;

      $nodes{$newnodeid} = $newnode;
    }

    ## Cleanup the dead links
    my $keep = 0;

    foreach my $t (qw/ff fr rf rr/)
    {
      if (defined $node->{$t})
      {
        my @newlinks;

        for (my $i = 0; $i < scalar @{$node->{$t}}; $i++)
        {
          my $nn = $node->{$t}->[$i];
          if (exists $portstatus{"$t:$nn"})
          {
            node_removelink($nodes{$nn}, $nodeid, flip_link($t));
          }
          else
          {
            push @newlinks, $nn;
          }
        }

        $keep += scalar @newlinks;

        if (scalar @newlinks)
        {
          $node->{$t} = \@newlinks;
        }
        else
        {
          $node->{$t} = undef;
        }
      }
    }

    if ($keep == 0)
    {
      ## This node has been fully resolved
      $node->{DEAD} = 1;
    }
  }

  while (($nodeid, $node) = each %nodes)
  {
    next if exists $node->{DEAD};

    print_node($nodeid, $node);
  }
}



while (<>)
{
  chomp;

  my @vals = split /\t/, $_;

  my $nodeid  = shift @vals;
  my $msgtype = shift @vals;

  my $node = {};

  if ($msgtype eq $NODEMSG)
  {
    parse_node($node, \@vals);
  }
  else
  {
    die "Unknown msg: $_\n";
  }

  $nodes{$nodeid} = $node;
}


resolveThreads();

